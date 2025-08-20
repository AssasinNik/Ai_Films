import os
import sys
import time
import json
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import pathlib
import logging

import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from pymongo import MongoClient

try:
    from elasticsearch import Elasticsearch  # type: ignore
    try:
        from elasticsearch.helpers import bulk as es_bulk  # type: ignore
    except Exception:
        es_bulk = None  # type: ignore
except Exception:
    Elasticsearch = None  # type: ignore
    es_bulk = None  # type: ignore

try:
    import redis
except Exception:
    redis = None  # type: ignore

try:
    from tqdm import tqdm  # type: ignore
except Exception:
    # graceful fallback if tqdm isn't installed
    def tqdm(iterable, total=None, desc=None, unit=None):
        return iterable

# -------------------------------
# Environment configuration
# -------------------------------

HOST_MONGO_URI = os.getenv("HOST_MONGO_URI", "mongodb://host.docker.internal:27017")
HOST_MONGO_DB = os.getenv("HOST_MONGO_DB", "catalog")
MOVIES_COLLECTION = os.getenv("MOVIES_COLLECTION", "movies")
PEOPLE_COLLECTION = os.getenv("PEOPLE_COLLECTION", "people")
SEASONS_COLLECTION = os.getenv("SEASONS_COLLECTION", "seasons")

PGHOST = os.getenv("PGHOST", "postgres")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGUSER = os.getenv("PGUSER", "movie_user")
PGPASSWORD = os.getenv("PGPASSWORD", os.getenv("POSTGRES_PASSWORD", "movie_password_2025"))
PGDATABASE = os.getenv("PGDATABASE", "movie_recommendation_db")

ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200")
ENABLE_ES = os.getenv("ENABLE_ES", "true").lower() in {"1", "true", "yes"}

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
ENABLE_REDIS = os.getenv("ENABLE_REDIS", "true").lower() in {"1", "true", "yes"}

# Cleanup toggles
FULL_CLEAN = os.getenv("FULL_CLEAN", "true").lower() in {"1", "true", "yes"}
TARGET_MONGO_URI = os.getenv("TARGET_MONGO_URI", "")
CLEAR_TARGET_MONGO = os.getenv("CLEAR_TARGET_MONGO", "false").lower() in {"1", "true", "yes"}

# If target cleanup requested but URI not provided, build a sensible default from compose
if CLEAR_TARGET_MONGO and not TARGET_MONGO_URI:
    _mongo_pw = os.getenv("MONGO_PASSWORD") or os.getenv("MONGO_INITDB_ROOT_PASSWORD") or "mongo_password_2025"
    TARGET_MONGO_URI = f"mongodb://movie_admin:{_mongo_pw}@mongodb:27017/movie_recommendation_db?authSource=admin"

# Performance settings
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))
COMMIT_INTERVAL = int(os.getenv("COMMIT_INTERVAL", "500"))

# Logging/progress settings
VERBOSE_LOGS = os.getenv("VERBOSE_LOGS", "false").lower() in {"1", "true", "yes"}
PROGRESS_ENABLED = os.getenv("PROGRESS_ENABLED", "true").lower() in {"1", "true", "yes"}

# External systems batching
ES_BULK_SIZE = int(os.getenv("ES_BULK_SIZE", "1000"))
REDIS_PIPELINE_SIZE = int(os.getenv("REDIS_PIPELINE_SIZE", "1000"))

# Redis trending configuration
TRENDING_MIN_RATING = float(os.getenv("TRENDING_MIN_RATING", "7.0"))
TRENDING_MIN_YEAR = int(os.getenv("TRENDING_MIN_YEAR", "2024"))
ALWAYS_CACHE_RECENT = os.getenv("ALWAYS_CACHE_RECENT", "true").lower() in {"1", "true", "yes"}

# Stage toggles
SKIP_PEOPLE = os.getenv("SKIP_PEOPLE", "false").lower() in {"1", "true", "yes"}
SKIP_MOVIES = os.getenv("SKIP_MOVIES", "false").lower() in {"1", "true", "yes"}
SKIP_SEASONS = os.getenv("SKIP_SEASONS", "false").lower() in {"1", "true", "yes"}


# -------------------------------
# Utilities
# -------------------------------

def log(msg: str) -> None:
    if not VERBOSE_LOGS:
        # Print only high-level or important messages detected by emojis/keywords
        important_markers = (
            "❌", "⚠️", "✅", "🚀", "📊", "🔍", "🔴", "🎯", "📦", "🐘", "📺", "🎬", "👥"
        )
        if not any(marker in msg for marker in important_markers):
            return
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{ts}] {msg}", flush=True)

def format_db_error(err: Exception) -> str:
    """Вернуть подробные сведения об ошибке транзакции/БД (тип, сообщение, PGCODE/PGERROR)."""
    try:
        base = f"{type(err).__name__}: {err}"
        pgcode = getattr(err, 'pgcode', None)
        pgerror = getattr(err, 'pgerror', None)
        parts = [base]
        if pgcode:
            parts.append(f"PGCODE={pgcode}")
        if pgerror:
            parts.append(f"PGERROR={str(pgerror).strip()}")
        return " | ".join(parts)
    except Exception:
        return str(err)

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    log("🔧 Logging system initialized")


def parse_mongo_date(value: Any) -> Optional[datetime]:
    """Parse Mongo Extended JSON date variants into datetime with UTC tzinfo.

    Handles:
    - {"$date": "2012-05-03T00:00:00.000Z"}
    - {"$date": {"$numberLong": "123456789000"}}
    - ISO string
    - None
    """
    if value is None:
        return None

    # Already a datetime
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

    # Extended JSON object
    if isinstance(value, dict) and "$date" in value:
        inner = value["$date"]
        if isinstance(inner, str):
            try:
                return datetime.fromisoformat(inner.replace("Z", "+00:00"))
            except Exception:
                return None
        if isinstance(inner, dict) and "$numberLong" in inner:
            try:
                millis = int(inner["$numberLong"])  # may be negative
                return datetime.fromtimestamp(millis / 1000.0, tz=timezone.utc)
            except Exception:
                return None

    # ISO string 
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None

    return None


def get_nested(d: Dict[str, Any], path: List[str], default: Any = None) -> Any:
    cur: Any = d
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def as_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        # Handle Mongo Extended JSON numeric types
        if isinstance(value, dict):
            for key in ("$numberInt", "$numberLong", "$numberDouble"):
                if key in value:
                    return int(float(str(value[key]).replace(",", ".")))
        if isinstance(value, str):
            return int(float(value.replace(",", ".")))
        return int(value)
    except Exception:
        return None


def as_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        if isinstance(value, dict):
            for key in ("$numberDouble", "$numberDecimal", "$numberInt", "$numberLong"):
                if key in value:
                    return float(str(value[key]).replace(",", "."))
        if isinstance(value, str):
            return float(value.replace(",", "."))
        return float(value)
    except Exception:
        return None


def normalize_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def truncate_text(value: Optional[str], max_length: int) -> Optional[str]:
    if value is None:
        return None
    return value[:max_length] if len(value) > max_length else value


def sanitize_year(value: Optional[int]) -> Optional[int]:
    if value is None:
        return None
    return value if 1800 <= value <= 2100 else None


def sanitize_age_rating(value: Optional[int]) -> Optional[int]:
    if value is None:
        return None
    return value if 0 <= value <= 21 else None


def sanitize_rating(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    try:
        # Some sources may send rating as string like "7,4" or out-of-range
        if isinstance(value, str):
            value = float(value.replace(",", "."))
    except Exception:
        return None
    return value if 0.0 <= float(value) <= 10.0 else None


# -------------------------------
# PostgreSQL operations
# -------------------------------

class PgRepo:
    def __init__(self) -> None:
        self.conn = None
        self._cache = {
            'countries': {},
            'genres': {},
            'roles': {},
            'distributors': {}
        }
        # Cache to avoid repeated person upserts within run: key -> person_id
        # Key is a tuple (name, en_name)
        self._person_cache: Dict[Tuple[Optional[str], Optional[str]], int] = {}
        # Statistics counters
        self.stats = {
            'people_inserted': 0,
            'people_updated': 0,
            'movies_inserted': 0,
            'movies_updated': 0,
            'seasons_inserted': 0,
            'seasons_updated': 0,
            'countries_created': 0,
            'genres_created': 0,
            'distributors_created': 0,
            'movie_countries_linked': 0,
            'movie_genres_linked': 0,
            'movie_people_linked': 0,
            'movie_facts_inserted': 0,
            'movie_videos_inserted': 0
        }

    def connect(self) -> None:
        retries = 12
        delay = 5
        for attempt in range(1, retries + 1):
            try:
                log(f"Attempting to connect to PostgreSQL at {PGHOST}:{PGPORT} as {PGUSER} to database {PGDATABASE}")
                self.conn = psycopg2.connect(
                    host=PGHOST,
                    port=PGPORT,
                    user=PGUSER,
                    password=PGPASSWORD,
                    dbname=PGDATABASE,
                )
                self.conn.autocommit = False
                # Optimize connection for bulk operations
                with self.conn.cursor() as cur:
                    cur.execute("SET work_mem = '256MB'")
                    cur.execute("SET maintenance_work_mem = '256MB'")
                    try:
                        cur.execute("SET synchronous_commit = off")
                    except Exception:
                        pass
                    # Note: fsync, synchronous_commit, full_page_writes require server restart
                    # These are commented out as they cannot be changed at runtime
                    # cur.execute("SET synchronous_commit = off")
                    # cur.execute("SET fsync = off")
                    # cur.execute("SET full_page_writes = off")
                log("✅ Connected to PostgreSQL with optimized settings")
                # Preload roles cache to avoid repeated SELECTs
                try:
                    with self.conn.cursor() as cur:
                        cur.execute("SELECT id, name FROM roles")
                        for rid, rname in cur.fetchall():
                            self._cache['roles'][rname] = rid
                except Exception:
                    pass
                return
            except Exception as e:
                log(f"⚠️  PostgreSQL connection attempt {attempt}/{retries} failed: {e}")
                if attempt < retries:
                    log(f"⏳ Waiting {delay} seconds before next attempt...")
                    time.sleep(delay)
        raise RuntimeError("Unable to connect to PostgreSQL")

    def close(self) -> None:
        if self.conn:
            self.conn.close()

    # Lookups / inserts with caching
    def _get_single_value(self, sql: str, params: Tuple[Any, ...]) -> Optional[int]:
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return row[0] if row else None

    def get_or_create_country(self, name: str) -> int:
        name = truncate_text(name.strip(), 100)
        if not name:
            return None
        
        # Check cache first
        if name in self._cache['countries']:
            return self._cache['countries'][name]
        
        country_id = self._get_single_value("SELECT id FROM countries WHERE name = %s", (name,))
        if country_id:
            self._cache['countries'][name] = country_id
            return country_id
        
        with self.conn.cursor() as cur:
            cur.execute("INSERT INTO countries(name) VALUES (%s) RETURNING id", (name,))
            country_id = cur.fetchone()[0]
            self._cache['countries'][name] = country_id
            self.stats['countries_created'] += 1
            if VERBOSE_LOGS:
                log(f"🌍 Created new country: {name} (ID: {country_id})")
        return country_id

    def get_or_create_genre(self, name: str) -> int:
        name = truncate_text(name.strip(), 100)
        if not name:
            return None
        
        # Check cache first
        if name in self._cache['genres']:
            return self._cache['genres'][name]
        
        genre_id = self._get_single_value("SELECT id FROM genres WHERE name = %s", (name,))
        if genre_id:
            self._cache['genres'][name] = genre_id
            return genre_id
        
        with self.conn.cursor() as cur:
            cur.execute("INSERT INTO genres(name) VALUES (%s) RETURNING id", (name,))
            genre_id = cur.fetchone()[0]
            self._cache['genres'][name] = genre_id
            self.stats['genres_created'] += 1
            if VERBOSE_LOGS:
                log(f"🎭 Created new genre: {name} (ID: {genre_id})")
        return genre_id

    def get_role_id(self, role_name: str) -> Optional[int]:
        if not role_name:
            return None
        
        # Check cache first
        if role_name in self._cache['roles']:
            return self._cache['roles'][role_name]
        
        role_id = self._get_single_value("SELECT id FROM roles WHERE name = %s", (role_name,))
        if role_id:
            self._cache['roles'][role_name] = role_id
        return role_id

    def upsert_person(self, person: Dict[str, Any]) -> Optional[int]:
        # Prefer local name, fallback to enName
        name = truncate_text(normalize_text(person.get("name")) or normalize_text(person.get("enName")), 200)
        en_name = truncate_text(normalize_text(person.get("enName")), 200)

        # If we cannot derive any non-null name, skip this person
        if not name:
            log("Skipping person without name fields")
            return None
        photo_url = normalize_text(person.get("photo"))

        # birthday could be {"$date": ...}
        birth_date_dt = parse_mongo_date(person.get("birthday"))
        birth_date = birth_date_dt.date() if birth_date_dt else None

        death_dt = parse_mongo_date(person.get("death"))
        death_date = death_dt.date() if death_dt else None

        birth_place_list = person.get("birthPlace") or []
        if isinstance(birth_place_list, list):
            birth_place = ", ".join(
                [normalize_text(item.get("value")) or "" for item in birth_place_list if isinstance(item, dict)]
            ).strip(", ")
        else:
            birth_place = None
        birth_place = truncate_text(birth_place, 200) if birth_place else None

        # Try to match by (name, en_name) using in-memory cache first
        cache_key = (name, en_name)
        if cache_key in self._person_cache:
            return self._person_cache[cache_key]

        # Try to match by (name, en_name) in DB
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM people WHERE name = %s AND COALESCE(en_name,'') = COALESCE(%s,'')",
                (name, en_name),
            )
            row = cur.fetchone()
            if row:
                person_id = row[0]
                cur.execute(
                    """
                    UPDATE people
                    SET photo_url = COALESCE(%s, photo_url),
                        birth_date = COALESCE(%s, birth_date),
                        death_date = COALESCE(%s, death_date),
                        birth_place = COALESCE(%s, birth_place),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    """,
                    (photo_url, birth_date, death_date, birth_place, person_id),
                )
                self.stats['people_updated'] += 1
                if VERBOSE_LOGS:
                    log(f"👤 Updated person: {name} (ID: {person_id})")
                self._person_cache[cache_key] = person_id
                return person_id

            # Insert new person
            cur.execute(
                """
                INSERT INTO people(name, en_name, birth_date, death_date, birth_place, photo_url)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (name, en_name, birth_date, death_date, birth_place, photo_url),
            )
            person_id = cur.fetchone()[0]
            self.stats['people_inserted'] += 1
            if VERBOSE_LOGS:
                log(f"👤 Inserted new person: {name} (ID: {person_id})")
            self._person_cache[cache_key] = person_id
            return person_id

    def upsert_distributor(self, name: Optional[str], release_name: Optional[str]) -> Optional[int]:
        if not name:
            return None
        name = truncate_text(name, 200)
        release_name = truncate_text(release_name, 200) if release_name else None
        
        cache_key = f"{name}:{release_name or ''}"
        if cache_key in self._cache['distributors']:
            return self._cache['distributors'][cache_key]
        
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM distributors WHERE name = %s AND COALESCE(release_name,'') = COALESCE(%s,'')",
                (name, release_name),
            )
            row = cur.fetchone()
            if row:
                self._cache['distributors'][cache_key] = row[0]
                return row[0]
            cur.execute(
                "INSERT INTO distributors(name, release_name) VALUES (%s, %s) RETURNING id",
                (name, release_name),
            )
            distributor_id = cur.fetchone()[0]
            self._cache['distributors'][cache_key] = distributor_id
            self.stats['distributors_created'] += 1
            if VERBOSE_LOGS:
                log(f"🎬 Created new distributor: {name} (ID: {distributor_id})")
            return distributor_id

    def insert_movie_core(self, movie: Dict[str, Any]) -> Optional[int]:
        explicit_id = as_int(movie.get("id"))
        title = normalize_text(movie.get("name")) or normalize_text(movie.get("alternativeName")) or normalize_text(movie.get("enName"))
        if not title:
            log("Skipping movie without any title fields")
            return None
        # Truncate potentially long text fields to fit schema constraints
        title = truncate_text(title, 500)
        alternative_name = truncate_text(normalize_text(movie.get("alternativeName")), 500)
        en_name = truncate_text(normalize_text(movie.get("enName")), 500)
        description = normalize_text(movie.get("description"))
        age_rating = sanitize_age_rating(as_int(movie.get("ageRating")))
        movie_length = as_int(movie.get("movieLength"))
        slogan = normalize_text(movie.get("slogan"))
        mtype = truncate_text(normalize_text(movie.get("type")) or "movie", 50)
        year = sanitize_year(as_int(movie.get("year")))

        premiere_world = parse_mongo_date(get_nested(movie, ["premiere", "world"]))
        premiere_russia = parse_mongo_date(get_nested(movie, ["premiere", "russia"]))

        rating = movie.get("rating") or {}
        rating_kp = sanitize_rating(as_float(rating.get("kp")))
        rating_imdb = sanitize_rating(as_float(rating.get("imdb")))
        rating_fc = sanitize_rating(as_float(rating.get("filmCritics")))
        rating_rfc = sanitize_rating(as_float(rating.get("russianFilmCritics")))

        votes = movie.get("votes") or {}
        votes_kp = as_int(votes.get("kp"))
        votes_imdb = as_int(votes.get("imdb"))
        votes_fc = as_int(votes.get("filmCritics"))
        votes_rfc = as_int(votes.get("russianFilmCritics"))

        budget = movie.get("budget") or {}
        budget_value = as_int(budget.get("value"))
        budget_currency = truncate_text(normalize_text(budget.get("currency")), 10)

        fees = movie.get("fees") or {}
        fees_world = fees.get("world") or {}
        fees_russia = fees.get("russia") or {}
        fees_usa = fees.get("usa") or {}

        fees_world_value = as_int(fees_world.get("value"))
        fees_world_currency = truncate_text(normalize_text(fees_world.get("currency")), 10)
        fees_russia_value = as_int(fees_russia.get("value"))
        fees_russia_currency = truncate_text(normalize_text(fees_russia.get("currency")), 10)
        fees_usa_value = as_int(fees_usa.get("value"))
        fees_usa_currency = truncate_text(normalize_text(fees_usa.get("currency")), 10)

        poster = movie.get("poster") or {}
        poster_url = normalize_text(poster.get("url"))
        poster_preview_url = normalize_text(poster.get("previewUrl"))

        backdrop = movie.get("backdrop") or {}
        backdrop_url = normalize_text(backdrop.get("url"))
        backdrop_preview_url = normalize_text(backdrop.get("previewUrl"))

        external = movie.get("externalId") or {}
        external_id_imdb = truncate_text(normalize_text(external.get("imdb")), 20)
        external_id_tmdb = as_int(external.get("tmdb"))
        external_id_kphd = truncate_text(normalize_text(external.get("kpHD")), 50)

        with self.conn.cursor() as cur:
            if explicit_id is not None:
                cur.execute(
                    """
                    INSERT INTO movies (
                        id,
                        title, alternative_name, en_name, description, age_rating, movie_length,
                        slogan, type, year, premiere_world, premiere_russia,
                        rating_kp, rating_imdb, rating_film_critics, rating_russian_film_critics,
                        votes_kp, votes_imdb, votes_film_critics, votes_russian_film_critics,
                        budget_value, budget_currency,
                        fees_world_value, fees_world_currency,
                        fees_russia_value, fees_russia_currency,
                        fees_usa_value, fees_usa_currency,
                        poster_url, poster_preview_url,
                        backdrop_url, backdrop_preview_url,
                        external_id_imdb, external_id_tmdb, external_id_kphd
                    ) VALUES (
                        %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s, %s
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        title = EXCLUDED.title,
                        alternative_name = EXCLUDED.alternative_name,
                        en_name = EXCLUDED.en_name,
                        description = EXCLUDED.description,
                        age_rating = EXCLUDED.age_rating,
                        movie_length = EXCLUDED.movie_length,
                        slogan = EXCLUDED.slogan,
                        type = EXCLUDED.type,
                        year = EXCLUDED.year,
                        premiere_world = EXCLUDED.premiere_world,
                        premiere_russia = EXCLUDED.premiere_russia,
                        rating_kp = EXCLUDED.rating_kp,
                        rating_imdb = EXCLUDED.rating_imdb,
                        rating_film_critics = EXCLUDED.rating_film_critics,
                        rating_russian_film_critics = EXCLUDED.rating_russian_film_critics,
                        votes_kp = EXCLUDED.votes_kp,
                        votes_imdb = EXCLUDED.votes_imdb,
                        votes_film_critics = EXCLUDED.votes_film_critics,
                        votes_russian_film_critics = EXCLUDED.votes_russian_film_critics,
                        budget_value = EXCLUDED.budget_value,
                        budget_currency = EXCLUDED.budget_currency,
                        fees_world_value = EXCLUDED.fees_world_value,
                        fees_world_currency = EXCLUDED.fees_world_currency,
                        fees_russia_value = EXCLUDED.fees_russia_value,
                        fees_russia_currency = EXCLUDED.fees_russia_currency,
                        fees_usa_value = EXCLUDED.fees_usa_value,
                        fees_usa_currency = EXCLUDED.fees_usa_currency,
                        poster_url = EXCLUDED.poster_url,
                        poster_preview_url = EXCLUDED.poster_preview_url,
                        backdrop_url = EXCLUDED.backdrop_url,
                        backdrop_preview_url = EXCLUDED.backdrop_preview_url,
                        external_id_imdb = EXCLUDED.external_id_imdb,
                        external_id_tmdb = EXCLUDED.external_id_tmdb,
                        external_id_kphd = EXCLUDED.external_id_kphd,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING id
                    """,
                    (
                        explicit_id,
                        title, alternative_name, en_name, description, age_rating, movie_length,
                        slogan, mtype, year,
                        premiere_world.date() if premiere_world else None,
                        premiere_russia.date() if premiere_russia else None,
                        rating_kp, rating_imdb, rating_fc, rating_rfc,
                        votes_kp, votes_imdb, votes_fc, votes_rfc,
                        budget_value, budget_currency,
                        fees_world_value, fees_world_currency,
                        fees_russia_value, fees_russia_currency,
                        fees_usa_value, fees_usa_currency,
                        poster_url, poster_preview_url,
                        backdrop_url, backdrop_preview_url,
                        external_id_imdb, external_id_tmdb, external_id_kphd,
                    ),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO movies (
                        title, alternative_name, en_name, description, age_rating, movie_length,
                        slogan, type, year, premiere_world, premiere_russia,
                        rating_kp, rating_imdb, rating_film_critics, rating_russian_film_critics,
                        votes_kp, votes_imdb, votes_film_critics, votes_russian_film_critics,
                        budget_value, budget_currency,
                        fees_world_value, fees_world_currency,
                        fees_russia_value, fees_russia_currency,
                        fees_usa_value, fees_usa_currency,
                        poster_url, poster_preview_url,
                        backdrop_url, backdrop_preview_url,
                        external_id_imdb, external_id_tmdb, external_id_kphd
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s, %s
                    ) RETURNING id
                    """,
                    (
                        title, alternative_name, en_name, description, age_rating, movie_length,
                        slogan, mtype, year,
                        premiere_world.date() if premiere_world else None,
                        premiere_russia.date() if premiere_russia else None,
                        rating_kp, rating_imdb, rating_fc, rating_rfc,
                        votes_kp, votes_imdb, votes_fc, votes_rfc,
                        budget_value, budget_currency,
                        fees_world_value, fees_world_currency,
                        fees_russia_value, fees_russia_currency,
                        fees_usa_value, fees_usa_currency,
                        poster_url, poster_preview_url,
                        backdrop_url, backdrop_preview_url,
                        external_id_imdb, external_id_tmdb, external_id_kphd,
                    ),
                )
            movie_id_row = cur.fetchone()
            movie_id = movie_id_row[0] if movie_id_row else None
            if movie_id:
                if explicit_id is not None:
                    self.stats['movies_updated'] += 1
                    if VERBOSE_LOGS:
                        log(f"🎬 Updated movie: {title} (ID: {movie_id}, Year: {year})")
                else:
                    self.stats['movies_inserted'] += 1
                    if VERBOSE_LOGS:
                        log(f"🎬 Inserted new movie: {title} (ID: {movie_id}, Year: {year})")
            return movie_id

    def link_movie_countries(self, movie_id: int, country_names: List[str]) -> None:
        if not country_names:
            return
        pairs: List[Tuple[int, int]] = []
        for name in country_names:
            if not name:
                continue
            cid = self.get_or_create_country(name)
            pairs.append((movie_id, cid))
        if pairs:
            with self.conn.cursor() as cur:
                execute_values(
                    cur,
                    "INSERT INTO movie_countries(movie_id, country_id) VALUES %s ON CONFLICT DO NOTHING",
                    pairs,
                )
                self.stats['movie_countries_linked'] += len(pairs)
                if VERBOSE_LOGS:
                    log(f"🌍 Linked {len(pairs)} countries to movie {movie_id}")

    def link_movie_genres(self, movie_id: int, genre_names: List[str]) -> None:
        if not genre_names:
            return
        pairs: List[Tuple[int, int]] = []
        for name in genre_names:
            if not name:
                continue
            gid = self.get_or_create_genre(name)
            pairs.append((movie_id, gid))
        if pairs:
            with self.conn.cursor() as cur:
                execute_values(
                    cur,
                    "INSERT INTO movie_genres(movie_id, genre_id) VALUES %s ON CONFLICT DO NOTHING",
                    pairs,
                )
                self.stats['movie_genres_linked'] += len(pairs)
                if VERBOSE_LOGS:
                    log(f"🎭 Linked {len(pairs)} genres to movie {movie_id}")

    def insert_movie_people(self, movie_id: int, persons: List[Dict[str, Any]]) -> None:
        if not persons:
            return
        rows: List[Tuple[int, int, Optional[int], Optional[str], Optional[int]]] = []
        order_index = 0
        seen_keys: set = set()
        for p in persons:
            order_index += 1
            person_id = self.upsert_person(p)
            if not person_id:
                continue
            role_key = (normalize_text(p.get("enProfession")) or "").lower()
            role_map = {
                "actor": "actor",
                "director": "director",
                "producer": "producer",
                "writer": "writer",
                "composer": "composer",
                "operator": "cinematographer",
                "cinematographer": "cinematographer",
                "editor": "editor",
                "production designer": "production_designer",
                "designer": "production_designer",
            }
            role_name = role_map.get(role_key)
            role_id = self.get_role_id(role_name) if role_name else None
            character_name = truncate_text(normalize_text(p.get("description")), 200)
            dedup_key = (person_id, role_id, character_name)
            if dedup_key in seen_keys:
                continue
            seen_keys.add(dedup_key)
            rows.append((movie_id, person_id, role_id, character_name, order_index))

        if rows:
            try:
                with self.conn.cursor() as cur:
                    execute_values(
                        cur,
                        """
                        INSERT INTO movie_people(movie_id, person_id, role_id, character_name, order_index)
                        VALUES %s
                        ON CONFLICT DO NOTHING
                        """,
                        rows,
                    )
                self.stats['movie_people_linked'] += len(rows)
                if VERBOSE_LOGS:
                    log(f"👥 Linked {len(rows)} people to movie {movie_id}")
            except Exception as batch_err:
                log(f"⚠️  Batch insert movie_people failed for movie {movie_id}: {format_db_error(batch_err)}. Fallback to per-row mode…")
                # Если батч рушится (например, из-за FK или переполнения поля), пробуем построчно
                linked = 0
                for r in rows:
                    try:
                        with self.conn.cursor() as cur:
                            cur.execute(
                                """
                                INSERT INTO movie_people(movie_id, person_id, role_id, character_name, order_index)
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT DO NOTHING
                                """,
                                r,
                            )
                        linked += 1
                    except Exception as row_err:
                        # пропускаем проблемную персону, фильм сохраняем
                        try:
                            mv_id, per_id, rid, ch_name, ord_idx = r
                        except Exception:
                            mv_id, per_id, rid, ch_name, ord_idx = (movie_id, None, None, None, None)
                        log(
                            f"⚠️  Skip movie_people link movie={mv_id}, person={per_id}, role={rid} due to error: {format_db_error(row_err)}"
                        )
                self.stats['movie_people_linked'] += linked
                log(f"👥 Linked {linked} people to movie {movie_id} (skipped {len(rows) - linked})")

    def insert_movie_facts(self, movie_id: int, facts: List[Dict[str, Any]]) -> None:
        if not facts:
            return
        rows: List[Tuple[int, str, str, bool]] = []
        for f in facts:
            text = normalize_text(f.get("value"))
            ftype = normalize_text(f.get("type"))
            spoiler = bool(f.get("spoiler", False))
            if text:
                rows.append((movie_id, text, (ftype or "FACT").upper(), spoiler))
        if rows:
            with self.conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO movie_facts(movie_id, fact_text, fact_type, is_spoiler)
                    VALUES %s
                    ON CONFLICT DO NOTHING
                    """,
                    rows,
                )
                self.stats['movie_facts_inserted'] += len(rows)
                if VERBOSE_LOGS:
                    log(f"📝 Inserted {len(rows)} facts for movie {movie_id}")

    def insert_movie_videos(self, movie_id: int, movie: Dict[str, Any]) -> None:
        videos = get_nested(movie, ["videos", "trailers"]) or []
        if not isinstance(videos, list) or not videos:
            return
        rows: List[Tuple[int, str, Optional[str], Optional[str], Optional[str]]] = []
        for v in videos:
            if not isinstance(v, dict):
                continue
            url = normalize_text(v.get("url"))
            if not url:
                continue
            name = normalize_text(v.get("name"))
            site = normalize_text(v.get("site"))
            vtype = normalize_text(v.get("type"))
            rows.append((movie_id, url, name, site, vtype.lower() if vtype else None))
        if rows:
            with self.conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO movie_videos(movie_id, video_url, video_name, video_site, video_type)
                    VALUES %s
                    ON CONFLICT DO NOTHING
                    """,
                    rows,
                )
                self.stats['movie_videos_inserted'] += len(rows)
                if VERBOSE_LOGS:
                    log(f"🎥 Inserted {len(rows)} videos for movie {movie_id}")

    def upsert_season(self, season_doc: Dict[str, Any]) -> Optional[int]:
        movie_id = as_int(season_doc.get("movieId"))
        if not movie_id:
            return None
        # Ensure movie exists (by id)
        with self.conn.cursor() as cur:
            cur.execute("SELECT id FROM movies WHERE id = %s", (movie_id,))
            exists = cur.fetchone() is not None
        if not exists:
            # Skip seasons without known movie
            log(f"Skipping season for unknown movie_id={movie_id}")
            return None

        number = as_int(season_doc.get("number")) or 1
        episodes_count = as_int(season_doc.get("episodesCount"))
        air_date_dt = parse_mongo_date(season_doc.get("airDate"))
        air_date = air_date_dt.date() if air_date_dt else None
        poster = season_doc.get("poster") or {}
        poster_url = normalize_text(poster.get("url"))
        description = normalize_text(season_doc.get("description"))

        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO seasons(movie_id, season_number, episodes_count, air_date, poster_url, description)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (movie_id, season_number) DO UPDATE SET
                    episodes_count = EXCLUDED.episodes_count,
                    air_date = EXCLUDED.air_date,
                    poster_url = EXCLUDED.poster_url,
                    description = EXCLUDED.description
                RETURNING id
                """,
                (movie_id, number, episodes_count, air_date, poster_url, description),
            )
            season_id = cur.fetchone()[0]
            self.stats['seasons_inserted'] += 1
            if VERBOSE_LOGS:
                log(f"📺 Inserted/updated season {number} for movie {movie_id} (ID: {season_id})")

        # Episodes
        episodes = season_doc.get("episodes") or []
        rows: List[Tuple[int, int, Optional[str], Optional[str], Optional[str], Optional[datetime], Optional[int], Optional[str], Optional[str]]] = []
        for ep in episodes:
            ep_num = as_int(ep.get("number")) or 0
            title = normalize_text(ep.get("name"))
            en_title = normalize_text(ep.get("enName"))
            synopsis = normalize_text(ep.get("description"))
            air_dt = parse_mongo_date(ep.get("airDate"))
            runtime = as_int(ep.get("duration"))
            still = ep.get("still") or {}
            still_url = normalize_text(still.get("url"))
            still_preview = normalize_text(still.get("previewUrl"))
            rows.append((season_id, ep_num, title, en_title, synopsis, air_dt.date() if air_dt else None, runtime, still_url, still_preview))

        if rows:
            with self.conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO episodes(
                        season_id, episode_number, title, en_title, synopsis, air_date, runtime, still_url, still_preview_url
                    ) VALUES %s
                    ON CONFLICT (season_id, episode_number) DO UPDATE SET
                        title = EXCLUDED.title,
                        en_title = EXCLUDED.en_title,
                        synopsis = EXCLUDED.synopsis,
                        air_date = EXCLUDED.air_date,
                        runtime = EXCLUDED.runtime,
                        still_url = EXCLUDED.still_url,
                        still_preview_url = EXCLUDED.still_preview_url
                    """,
                    rows,
                )
                if VERBOSE_LOGS:
                    log(f"📺 Inserted {len(rows)} episodes for season {season_id}")
        return season_id


# -------------------------------
# Elasticsearch operations
# -------------------------------

class EsRepo:
    def __init__(self, url: str) -> None:
        self.client = None
        self.url = url
        self._buffer: List[Dict[str, Any]] = []
        self._bulk_size = ES_BULK_SIZE

    def connect(self) -> None:
        if not ENABLE_ES or Elasticsearch is None:
            log("Elasticsearch disabled or client not available; skipping ES indexing")
            return
        retries = 12
        delay = 5
        for attempt in range(1, retries + 1):
            try:
                log(f"Elasticsearch connection attempt {attempt}/{retries} to {self.url}")
                # Force compatibility headers with ES 8 cluster when using newer client
                default_headers = {
                    "accept": "application/vnd.elasticsearch+json; compatible-with=8",
                    "content-type": "application/vnd.elasticsearch+json; compatible-with=8",
                }
                self.client = Elasticsearch(
                    self.url, 
                    verify_certs=False, 
                    headers=default_headers,
                    request_timeout=30,
                    max_retries=3
                )
                # Cheap health ping
                info = self.client.info()
                log(f"Connected to Elasticsearch version {info.get('version', {}).get('number', 'unknown')}")
                return
            except Exception as e:
                log(f"Elasticsearch connection attempt {attempt}/{retries} failed: {e}")
                if attempt < retries:
                    log(f"Waiting {delay} seconds before next attempt...")
                    time.sleep(delay)
        log("Failed to connect to Elasticsearch after all attempts")

    def index_movie(self, movie_id: int, body: Dict[str, Any]) -> None:
        if not self.client:
            return
        index_name = "movies"
        try:
            if es_bulk is None:
                # fallback to single index if helpers not available
                self.client.index(index=index_name, id=movie_id, document=body, refresh="false")
                return
            action = {
                "_index": index_name,
                "_id": movie_id,
                "_op_type": "index",
                "_source": body,
            }
            self._buffer.append(action)
            if len(self._buffer) >= self._bulk_size:
                self.flush()
        except Exception as e:
            log(f"⚠️  Failed to index movie {movie_id} into ES: {e}")

    def flush(self) -> None:
        if not self.client or not self._buffer or es_bulk is None:
            # nothing to flush or unsupported
            self._buffer.clear()
            return
        try:
            es_bulk(self.client, self._buffer, refresh=False, request_timeout=60)
        except Exception as e:
            log(f"⚠️  ES bulk flush failed: {e}")
        finally:
            self._buffer.clear()

    def close(self) -> None:
        try:
            self.flush()
        except Exception:
            pass

    # ---- Setup helpers ----
    def put_index_template(self, name: str, body: Dict[str, Any]) -> None:
        if not self.client:
            return
        try:
            self.client.indices.put_index_template(name=name, body=body)
        except Exception as e:
            log(f"⚠️  Failed to put index template {name}: {e}")

    def put_ilm_policy(self, name: str, policy: Dict[str, Any]) -> None:
        if not self.client:
            return
        try:
            self.client.ilm.put_lifecycle(name=name, policy=policy)
        except Exception as e:
            log(f"⚠️  Failed to put ILM policy {name}: {e}")

    def put_stored_script(self, name: str, script: Dict[str, Any]) -> None:
        if not self.client:
            return
        try:
            scr = script.get("script") if isinstance(script, dict) else None
            if scr is None:
                scr = script
            self.client.put_script(id=name, script=scr)
        except Exception as e:
            log(f"⚠️  Failed to put stored script {name}: {e}")


# -------------------------------
# Redis operations
# -------------------------------

class RedisRepo:
    def __init__(self, url: str) -> None:
        self.client = None
        self.url = url
        self._pipeline = None
        self._pending = 0

    def connect(self) -> None:
        if not ENABLE_REDIS or redis is None:
            log("Redis disabled or client not available; skipping Redis caching")
            return
        retries = 12
        delay = 5
        for attempt in range(1, retries + 1):
            try:
                self.client = redis.from_url(self.url, decode_responses=True)
                # Test connection
                self.client.ping()
                log("✅ Connected to Redis")
                return
            except Exception as e:
                log(f"⚠️  Redis connection attempt {attempt}/{retries} failed: {e}")
                if attempt < retries:
                    log(f"⏳ Waiting {delay} seconds before next attempt...")
                    time.sleep(delay)

    def cache_trending_movie(self, movie_id: int, movie_data: Dict[str, Any]) -> None:
        """Кэшировать трендовые фильмы в Redis по настраиваемым порогам."""
        if not self.client:
            return
        
        try:
            # Check if movie meets criteria
            rating = movie_data.get("rating", {})
            rating_kp = as_float(rating.get("kp"))
            rating_imdb = as_float(rating.get("imdb"))
            
            # Use highest available rating
            max_rating = max(filter(None, [rating_kp, rating_imdb])) if any([rating_kp, rating_imdb]) else 0
            
            year = as_int(movie_data.get("year"))
            # Сохраняем в Redis только свежие фильмы (>= TRENDING_MIN_YEAR)
            if year is None or year < TRENDING_MIN_YEAR:
                return

            # Criteria: rating threshold (год уже проверен выше)
            meets_thresholds = (max_rating >= TRENDING_MIN_RATING)
            if meets_thresholds or ALWAYS_CACHE_RECENT:
                # Create movie summary for Redis
                raw_summary = {
                    "id": movie_id,
                    "title": movie_data.get("name") or movie_data.get("alternativeName") or movie_data.get("enName"),
                    "year": year,
                    "rating_kp": rating_kp,
                    "rating_imdb": rating_imdb,
                    "max_rating": max_rating,
                    "type": movie_data.get("type") or "movie",
                    "poster_url": get_nested(movie_data, ["poster", "url"]),
                    "description": movie_data.get("description"),
                    "genres": [g.get("name") for g in (movie_data.get("genres") or []) if isinstance(g, dict)],
                    "countries": [c.get("name") for c in (movie_data.get("countries") or []) if isinstance(c, dict)],
                    "cached_at": datetime.now(timezone.utc).isoformat()
                }
                def _to_redis_str(v: Any) -> str:
                    if v is None:
                        return ""
                    if isinstance(v, (list, dict)):
                        return json.dumps(v, ensure_ascii=False)
                    return str(v)
                movie_summary = {k: _to_redis_str(v) for k, v in raw_summary.items()}
                
                # Use pipeline for batching
                pipe = self._pipeline or self.client.pipeline(transaction=False)
                self._pipeline = pipe

                movie_key = f"movie:trending:{movie_id}"
                pipe.hset(movie_key, mapping=movie_summary)
                trending_key = "movies:trending:high_rated"
                if meets_thresholds:
                    pipe.zadd(trending_key, {str(movie_id): float(max_rating or 0)})
                recent_key = "movies:trending:recent"
                pipe.zadd(recent_key, {str(movie_id): int(year or 0)})
                pipe.expire(movie_key, 30 * 24 * 3600)
                pipe.expire(trending_key, 30 * 24 * 3600)
                pipe.expire(recent_key, 30 * 24 * 3600)

                self._pending += 1
                if self._pending >= REDIS_PIPELINE_SIZE:
                    self.flush()

                if VERBOSE_LOGS:
                    log(f"🔴 Cached trending movie {movie_id} (rating: {max_rating}, year: {year})")
                
        except Exception as e:
            log(f"⚠️  Failed to cache movie {movie_id} in Redis: {e}")

    def flush(self) -> None:
        if not self.client or not self._pipeline:
            self._pending = 0
            self._pipeline = None
            return
        try:
            self._pipeline.execute()
        except Exception as e:
            log(f"⚠️  Redis pipeline flush failed: {e}")
        finally:
            self._pending = 0
            self._pipeline = None

    def close(self) -> None:
        try:
            self.flush()
        except Exception:
            pass

    def get_trending_movies(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get trending movies from Redis"""
        if not self.client:
            return []
        
        try:
            # Get top rated movies
            trending_ids = self.client.zrevrange("movies:trending:high_rated", 0, limit - 1)
            movies = []
            
            for movie_id in trending_ids:
                movie_key = f"movie:trending:{movie_id}"
                movie_data = self.client.hgetall(movie_key)
                if movie_data:
                    movies.append(movie_data)
            
            return movies
        except Exception as e:
            log(f"⚠️  Failed to get trending movies from Redis: {e}")
            return []

    def get_recent_movies(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent movies from Redis"""
        if not self.client:
            return []
        
        try:
            # Get most recent movies
            recent_ids = self.client.zrevrange("movies:trending:recent", 0, limit - 1)
            movies = []
            
            for movie_id in recent_ids:
                movie_key = f"movie:trending:{movie_id}"
                movie_data = self.client.hgetall(movie_key)
                if movie_data:
                    movies.append(movie_data)
            
            return movies
        except Exception as e:
            log(f"⚠️  Failed to get recent movies from Redis: {e}")
            return []


# -------------------------------
# Main seeding flow
# -------------------------------

def transform_movie_to_es(movie_id: int, movie_doc: Dict[str, Any]) -> Dict[str, Any]:
    genres = movie_doc.get("genres") or []
    countries = movie_doc.get("countries") or []
    persons = movie_doc.get("persons") or []
    rating = movie_doc.get("rating") or {}
    votes = movie_doc.get("votes") or {}
    poster = movie_doc.get("poster") or {}
    backdrop = movie_doc.get("backdrop") or {}

    return {
        "movie_id": movie_id,
        "title": movie_doc.get("name"),
        "alternative_name": movie_doc.get("alternativeName"),
        "en_name": movie_doc.get("enName"),
        "description": movie_doc.get("description"),
        "year": movie_doc.get("year"),
        "genres": [{"name": g.get("name")} for g in genres if isinstance(g, dict)],
        "countries": [{"name": c.get("name")} for c in countries if isinstance(c, dict)],
        "people": [
            {
                "name": p.get("name"),
                "role": p.get("enProfession") or p.get("profession"),
                "character_name": p.get("description"),
            }
            for p in persons if isinstance(p, dict)
        ],
        "ratings": {
            "kp": rating.get("kp"),
            "imdb": rating.get("imdb"),
        },
        "votes": {
            "kp": votes.get("kp"),
            "imdb": votes.get("imdb"),
        },
        "movie_length": movie_doc.get("movieLength"),
        "age_rating": movie_doc.get("ageRating"),
        "type": movie_doc.get("type") or "movie",
        "poster_url": poster.get("url"),
        "backdrop_url": backdrop.get("url"),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def process_people_batch(pg: PgRepo, people_batch: List[Dict[str, Any]]) -> int:
    """Process a batch of people documents"""
    inserted = 0
    skipped = 0
    
    for person_doc in people_batch:
        sp_name = f"sp_person_{time.time_ns()}"
        try:
            with pg.conn.cursor() as cur:
                cur.execute(f"SAVEPOINT {sp_name}")
            pid = pg.upsert_person(person_doc)
            if pid:
                inserted += 1
            with pg.conn.cursor() as cur:
                cur.execute(f"RELEASE SAVEPOINT {sp_name}")
        except Exception as e:
            skipped += 1
            try:
                with pg.conn.cursor() as cur:
                    cur.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                    cur.execute(f"RELEASE SAVEPOINT {sp_name}")
            except Exception:
                pass
            person_id = person_doc.get('_id', 'unknown')
            log(f"⚠️  Skip person {person_id} due to error: {format_db_error(e)}")
    
    if skipped > 0:
        log(f"⚠️  Skipped {skipped} people due to errors in batch")
    
    return inserted

def process_movie_batch(pg: PgRepo, es: EsRepo, redis_repo: RedisRepo, movies_batch: List[Dict[str, Any]]) -> int:
    """Process a batch of movie documents"""
    inserted = 0
    skipped = 0
    es_indexed = 0
    redis_cached = 0
    
    for movie_doc in movies_batch:
        sp_name = f"sp_movie_{time.time_ns()}"
        try:
            with pg.conn.cursor() as cur:
                cur.execute(f"SAVEPOINT {sp_name}")

            movie_id = pg.insert_movie_core(movie_doc)
            if not movie_id:
                skipped += 1
                with pg.conn.cursor() as cur:
                    cur.execute(f"RELEASE SAVEPOINT {sp_name}")
                continue

            # Countries
            countries = [normalize_text(c.get("name")) for c in (movie_doc.get("countries") or []) if isinstance(c, dict)]
            pg.link_movie_countries(movie_id, [c for c in countries if c])

            # Genres
            genres = [normalize_text(g.get("name")) for g in (movie_doc.get("genres") or []) if isinstance(g, dict)]
            pg.link_movie_genres(movie_id, [g for g in genres if g])

            # People links
            persons = movie_doc.get("persons") or []
            if isinstance(persons, list):
                pg.insert_movie_people(movie_id, persons)

            # Facts
            pg.insert_movie_facts(movie_id, movie_doc.get("facts") or [])

            # Videos
            pg.insert_movie_videos(movie_id, movie_doc)

            # Distributors
            distributors = movie_doc.get("distributors") or {}
            d_name = normalize_text(distributors.get("distributor"))
            d_release = normalize_text(distributors.get("distributorRelease"))
            distributor_id = pg.upsert_distributor(d_name, d_release)
            if distributor_id:
                with pg.conn.cursor() as cur:
                    release_dt = parse_mongo_date(get_nested(movie_doc, ["premiere", "russia"])) or \
                                 parse_mongo_date(get_nested(movie_doc, ["premiere", "world"]))
                    cur.execute(
                        """
                        INSERT INTO movie_distributors(movie_id, distributor_id, distribution_type, release_date)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (
                            movie_id,
                            distributor_id,
                            "theatrical",
                            release_dt.date() if release_dt else None,
                        ),
                    )

            # ES index
            if es and es.client:
                try:
                    es_doc = transform_movie_to_es(movie_id, movie_doc)
                    es.index_movie(movie_id, es_doc)
                    es_indexed += 1
                except Exception as e:
                    log(f"⚠️  ES index skipped for movie {movie_id}: {e}")

            # Redis cache trending movies
            if redis_repo and redis_repo.client:
                try:
                    redis_repo.cache_trending_movie(movie_id, movie_doc)
                    redis_cached += 1
                except Exception as e:
                    log(f"⚠️  Redis cache skipped for movie {movie_id}: {e}")

            inserted += 1
            with pg.conn.cursor() as cur:
                cur.execute(f"RELEASE SAVEPOINT {sp_name}")
        except Exception as e:
            skipped += 1
            try:
                with pg.conn.cursor() as cur:
                    cur.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                    cur.execute(f"RELEASE SAVEPOINT {sp_name}")
            except Exception:
                pass
            movie_id = movie_doc.get('_id', 'unknown')
            log(f"⚠️  Skip movie {movie_id} due to error: {format_db_error(e)}")
    
    if skipped > 0:
        log(f"⚠️  Skipped {skipped} movies due to errors in batch")
    if es_indexed > 0:
        log(f"🔍 Indexed {es_indexed} movies to Elasticsearch in batch")
    if redis_cached > 0:
        log(f"🔴 Cached {redis_cached} movies to Redis in batch")
    
    return inserted

def seed_from_mongo() -> None:
    setup_logging()
    start_time = time.time()
    
    log("🚀 Starting data initialization service...")
    log(f"📊 Performance settings: BATCH_SIZE={BATCH_SIZE}, MAX_WORKERS={MAX_WORKERS}, COMMIT_INTERVAL={COMMIT_INTERVAL}")
    log(f"🔴 Redis caching: {'enabled' if ENABLE_REDIS else 'disabled'}")
    log(f"🔍 Elasticsearch indexing: {'enabled' if ENABLE_ES else 'disabled'}")
    log(f"📦 MongoDB source: {HOST_MONGO_URI}/{HOST_MONGO_DB}")
    log(f"🐘 PostgreSQL target: {PGHOST}:{PGPORT}/{PGDATABASE}")
    log("=" * 80)
    
    # Connect to Mongo
    log(f"📦 Connecting to MongoDB at {HOST_MONGO_URI}, db={HOST_MONGO_DB}")
    try:
        mclient = MongoClient(HOST_MONGO_URI, serverSelectionTimeoutMS=10000)
        mdb = mclient[HOST_MONGO_DB]
        # Test MongoDB connection
        mdb.command('ping')
        log("✅ MongoDB connection successful")
        col_movies = mdb[MOVIES_COLLECTION]
        col_people = mdb[PEOPLE_COLLECTION]
        col_seasons = mdb[SEASONS_COLLECTION]
    except Exception as e:
        log(f"❌ Failed to connect to MongoDB: {e}")
        raise

    # Connect to Postgres
    log("🐘 Connecting to PostgreSQL...")
    pg = PgRepo()
    pg.connect()

    # Connect to Elasticsearch (optional)
    es = None
    if ENABLE_ES and Elasticsearch:
        log(f"🔍 Connecting to Elasticsearch at {ELASTICSEARCH_URL}")
        try:
            es = EsRepo(ELASTICSEARCH_URL)
            es.connect()
            log("✅ Elasticsearch connection successful")
            # Apply Elasticsearch setup
            try:
                # Prefer mounted /schemas in container, fallback to repo path
                mounted = pathlib.Path('/schemas/elasticsearch_setup.json')
                setup_path = mounted if mounted.exists() else (pathlib.Path(__file__).resolve().parents[1] / 'schemas' / 'elasticsearch_setup.json')
                if setup_path.exists():
                    with open(setup_path, 'r', encoding='utf-8') as f:
                        import json as _json
                        setup = _json.load(f).get('elasticsearch_setup', {})
                    # Helpers to massage setup JSON into ES 8 API shapes
                    def to_composable_index_template(tpl: Dict[str, Any]) -> Dict[str, Any]:
                        tpl = dict(tpl or {})
                        index_patterns = tpl.get('index_patterns') or tpl.get('indexPatterns') or []
                        template_block: Dict[str, Any] = {}
                        if 'settings' in tpl:
                            template_block['settings'] = tpl['settings']
                        if 'mappings' in tpl:
                            template_block['mappings'] = tpl['mappings']
                        if 'aliases' in tpl:
                            template_block['aliases'] = tpl['aliases']
                        # Remove moved keys
                        for k in ['settings', 'mappings', 'aliases']:
                            tpl.pop(k, None)
                        # Compose final body
                        body: Dict[str, Any] = {
                            'index_patterns': index_patterns,
                            'template': template_block
                        }
                        # Carry optional fields
                        for opt in ['priority', 'version', '_meta']:
                            if opt in tpl:
                                body[opt] = tpl[opt]
                        return body

                    def fix_ilm_policy(policy: Dict[str, Any]) -> Dict[str, Any]:
                        pol = dict(policy or {})
                        phases = pol.get('phases') or {}
                        for phase_name, phase in list(phases.items()):
                            if not isinstance(phase, dict):
                                continue
                            if 'actions' not in phase:
                                phase['actions'] = {}
                            # If this is delete phase and no delete action specified
                            if phase_name == 'delete' and 'delete' not in phase['actions']:
                                phase['actions']['delete'] = {}
                            phases[phase_name] = phase
                        pol['phases'] = phases
                        return pol

                    # Try applying as-is, then fallback to sanitized if hunspell missing
                    def sanitize_template(tpl: Dict[str, Any]) -> Dict[str, Any]:
                        tpl = dict(tpl or {})
                        settings = tpl.get('settings') or {}
                        analysis = settings.get('analysis') or {}
                        filters = analysis.get('filter') or {}
                        if 'russian_morphology' in filters:
                            filters.pop('russian_morphology', None)
                            analysis['filter'] = filters
                        analyzers = analysis.get('analyzer') or {}
                        if 'russian_analyzer' in analyzers:
                            ra = analyzers['russian_analyzer']
                            ra['tokenizer'] = 'standard'
                            ra['filter'] = ['lowercase', 'snowball_russian', 'russian_stop']
                            analyzers['russian_analyzer'] = ra
                            analysis['analyzer'] = analyzers
                        if analysis:
                            settings['analysis'] = analysis
                            tpl['settings'] = settings
                        return tpl

                    for name, raw in (setup.get('index_templates') or {}).items():
                        tpl = to_composable_index_template(raw)
                        try:
                            es.put_index_template(name, tpl)
                            log(f"✅ Applied ES index template: {name}")
                        except Exception as e:
                            log(f"⚠️  Template {name} failed as-is, retry with sanitized analysis: {e}")
                            es.put_index_template(name, to_composable_index_template(sanitize_template(raw)))
                            log(f"✅ Applied ES index template (sanitized): {name}")
                    for name, pol in (setup.get('index_lifecycle_policies') or {}).items():
                        final_policy = fix_ilm_policy(pol.get('policy') or {})
                        try:
                            es.put_ilm_policy(name, final_policy)
                            log(f"✅ Applied ES ILM policy: {name}")
                        except Exception as pe:
                            log(f"⚠️  ILM policy {name} failed: {pe}")
                    for name, scr in (setup.get('search_templates') or {}).items():
                        es.put_stored_script(name, scr)
                        log(f"✅ Applied ES stored script: {name}")
                else:
                    log("⚠️  schemas/elasticsearch_setup.json not found, skipping ES setup")
            except Exception as ee:
                log(f"⚠️  ES setup application failed: {ee}")
        except Exception as e:
            log(f"⚠️  Failed to connect to Elasticsearch: {e}")
            log("⚠️  Continuing without Elasticsearch indexing...")
            es = None

    # Connect to Redis (optional)
    redis_repo = None
    if ENABLE_REDIS and redis:
        log(f"🔴 Connecting to Redis at {REDIS_URL}")
        try:
            redis_repo = RedisRepo(REDIS_URL)
            redis_repo.connect()
            log("✅ Redis connection successful")
        except Exception as e:
            log(f"⚠️  Failed to connect to Redis: {e}")
            log("⚠️  Continuing without Redis caching...")
            redis_repo = None

    # Optional full cleanup before seeding
    try:
        if FULL_CLEAN:
            log("🧹 Performing full cleanup before seeding…")
            # Clean Redis
            if redis_repo and redis_repo.client:
                try:
                    redis_repo.client.flushdb()
                    log("🧹 Redis FLUSHDB completed")
                except Exception as e:
                    log(f"⚠️  Redis cleanup failed: {e}")
            # Clean Elasticsearch
            if es and es.client:
                try:
                    if es.client.indices.exists(index="movies"):
                        es.client.indices.delete(index="movies", ignore=[400, 404])
                        log("🧹 Elasticsearch index 'movies' deleted")
                except Exception as e:
                    log(f"⚠️  Elasticsearch cleanup failed: {e}")
            # Clean PostgreSQL data
            try:
                with pg.conn.cursor() as cur:
                    cur.execute(
                        """
                        TRUNCATE TABLE
                          movie_people,
                          movie_genres,
                          movie_countries,
                          movie_distributors,
                          movie_facts,
                          movie_videos,
                          movie_emotional_tags,
                          movie_mood_categories,
                          episodes,
                          seasons,
                          reviews,
                          movies
                        RESTART IDENTITY CASCADE
                        """
                    )
                    pg.conn.commit()
                    log("🧹 PostgreSQL tables truncated (with CASCADE)")
            except Exception as e:
                log(f"⚠️  PostgreSQL cleanup failed: {format_db_error(e)}")
            # Optionally clean target MongoDB (user-facing DB)
            if CLEAR_TARGET_MONGO and TARGET_MONGO_URI:
                try:
                    tmc = MongoClient(TARGET_MONGO_URI, serverSelectionTimeoutMS=10000)
                    # If URI without db, skip
                    try:
                        tdb = tmc.get_database()
                    except Exception:
                        tdb = None
                    if tdb is not None:
                        for col in ["users", "emotional_profiles", "viewing_history", "user_recommendations", "mood_detection_sessions", "user_feedback"]:
                            try:
                                tdb[col].drop()
                            except Exception:
                                pass
                        log("🧹 Target MongoDB collections dropped")
                    tmc.close()
                except Exception as e:
                    log(f"⚠️  Target MongoDB cleanup failed: {e}")
            log("✅ Cleanup completed")
    except Exception as e:
        log(f"⚠️  Cleanup stage error (continuing): {e}")

    inserted_movies = 0
    inserted_people = 0
    inserted_seasons = 0
    cached_trending = 0

    try:
        # People: load standalone people first so that later links can reuse
        if SKIP_PEOPLE:
            log("👥 Seeding people skipped by config")
        else:
            log("👥 Seeding people from MongoDB...")
        people_total: Optional[int] = None
        try:
            people_total = col_people.estimated_document_count()
        except Exception:
            try:
                people_total = col_people.count_documents({})
            except Exception:
                people_total = None
        pbar_people = tqdm(total=people_total, desc="People ➜ PostgreSQL", unit="doc") if PROGRESS_ENABLED else None
        people_cursor = col_people.find({}, projection=None).batch_size(BATCH_SIZE)
        people_batch: List[Dict[str, Any]] = []
        total_people_batches = 0
        processed_people = 0
        for person_doc in people_cursor:
            people_batch.append(person_doc)
            if len(people_batch) >= BATCH_SIZE:
                total_people_batches += 1
                batch_start = time.time()
                inserted_people += process_people_batch(pg, people_batch)
                processed_people += len(people_batch)
                if pbar_people:
                    pbar_people.update(len(people_batch))
                people_batch = []
                pg.conn.commit()
                batch_time = time.time() - batch_start
                log(f"✅ Committed people batch {total_people_batches} ({inserted_people} total) in {batch_time:.2f}s")
        if people_batch:
            total_people_batches += 1
            batch_start = time.time()
            inserted_people += process_people_batch(pg, people_batch)
            processed_people += len(people_batch)
            if pbar_people:
                pbar_people.update(len(people_batch))
            people_batch = []
            pg.conn.commit()
            batch_time = time.time() - batch_start
            log(f"✅ Committed people batch {total_people_batches} ({inserted_people} total) in {batch_time:.2f}s")
            if pbar_people:
                pbar_people.close()
            
            log(f"🎉 Inserted/updated {inserted_people} people")
            log(f"   📊 People stats: {pg.stats['people_inserted']} inserted, {pg.stats['people_updated']} updated")

        # Movies
        if SKIP_MOVIES:
            log("🎬 Seeding movies skipped by config")
        else:
            log("🎬 Seeding movies from MongoDB...")
        movies_total: Optional[int] = None
        try:
            movies_total = col_movies.estimated_document_count()
        except Exception:
            try:
                movies_total = col_movies.count_documents({})
            except Exception:
                movies_total = None
        pbar_movies = tqdm(total=movies_total, desc="Movies ➜ PostgreSQL/ES/Redis", unit="doc") if PROGRESS_ENABLED else None
        movies_cursor = col_movies.find({}, projection=None).batch_size(BATCH_SIZE)
        movies_batch: List[Dict[str, Any]] = []
        total_movie_batches = 0
        for movie_doc in movies_cursor:
            movies_batch.append(movie_doc)
            if len(movies_batch) >= BATCH_SIZE:
                total_movie_batches += 1
                batch_start = time.time()
                inserted_movies += process_movie_batch(pg, es, redis_repo, movies_batch)
                if pbar_movies:
                    pbar_movies.update(len(movies_batch))
                movies_batch = []
                pg.conn.commit()
                # flush ES/Redis buffers
                if es:
                    es.flush()
                if redis_repo:
                    redis_repo.flush()
                batch_time = time.time() - batch_start
                log(f"✅ Committed movies batch {total_movie_batches} ({inserted_movies} total) in {batch_time:.2f}s")
        if movies_batch:
            total_movie_batches += 1
            batch_start = time.time()
            inserted_movies += process_movie_batch(pg, es, redis_repo, movies_batch)
            if pbar_movies:
                pbar_movies.update(len(movies_batch))
            movies_batch = []
            pg.conn.commit()
            if es:
                es.flush()
            if redis_repo:
                redis_repo.flush()
            batch_time = time.time() - batch_start
            log(f"✅ Committed movies batch {total_movie_batches} ({inserted_movies} total) in {batch_time:.2f}s")
            if pbar_movies:
                pbar_movies.close()

            log(f"🎉 Inserted movies: {inserted_movies}")
            log(f"   📊 Movies stats: {pg.stats['movies_inserted']} inserted, {pg.stats['movies_updated']} updated")
            log(f"   🔗 Relationships: {pg.stats['movie_countries_linked']} countries, {pg.stats['movie_genres_linked']} genres, {pg.stats['movie_people_linked']} people")
            log(f"   📝 Content: {pg.stats['movie_facts_inserted']} facts, {pg.stats['movie_videos_inserted']} videos")

        # Log Redis caching summary
        if redis_repo and redis_repo.client:
            try:
                trending_count = redis_repo.client.zcard("movies:trending:high_rated")
                recent_count = redis_repo.client.zcard("movies:trending:recent")
                log(f"🔴 Redis caching completed: {trending_count} trending movies, {recent_count} recent movies cached")
            except Exception as e:
                log(f"⚠️  Failed to get Redis stats: {e}")

        # Seasons (only if movie exists)
        if SKIP_SEASONS:
            log("📺 Seeding seasons skipped by config")
        else:
            log("📺 Seeding seasons from MongoDB...")
        seasons_total: Optional[int] = None
        try:
            seasons_total = col_seasons.estimated_document_count()
        except Exception:
            try:
                seasons_total = col_seasons.count_documents({})
            except Exception:
                seasons_total = None
        pbar_seasons = tqdm(total=seasons_total, desc="Seasons ➜ PostgreSQL", unit="doc") if PROGRESS_ENABLED else None
        seasons_cursor = col_seasons.find({}, projection=None).batch_size(BATCH_SIZE)
        processed_since_commit = 0
        for season_doc in seasons_cursor:
            sp_name = f"sp_season_{time.time_ns()}"
            try:
                # Создаем сейвпоинт
                with pg.conn.cursor() as cur:
                    cur.execute(f"SAVEPOINT {sp_name}")

                # Пытаемся вставить/обновить сезон и эпизоды
                sid = pg.upsert_season(season_doc)
                if sid:
                    inserted_seasons += 1
                    processed_since_commit += 1

                # Сначала освобождаем сейвпоинт, чтобы не ломать после commit
                with pg.conn.cursor() as cur:
                    cur.execute(f"RELEASE SAVEPOINT {sp_name}")

                if pbar_seasons:
                    pbar_seasons.update(1)

                # Коммитим партиями уже после RELEASE SAVEPOINT
                if processed_since_commit >= COMMIT_INTERVAL:
                    pg.conn.commit()
                    log(f"✅ Committed {inserted_seasons} seasons so far...")
                    processed_since_commit = 0
            except Exception as e:
                # Пытаемся откатиться к сейвпоинту, если он есть; если нет — полный rollback
                try:
                    with pg.conn.cursor() as cur:
                        cur.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                        cur.execute(f"RELEASE SAVEPOINT {sp_name}")
                except Exception:
                    try:
                        pg.conn.rollback()
                    except Exception:
                        pass
                log(f"⚠️  Skip season {season_doc.get('_id')} due to error: {format_db_error(e)}")
        
        pg.conn.commit()
        if pbar_seasons:
            pbar_seasons.close()
        log(f"🎉 Inserted/updated seasons: {inserted_seasons}")
        log(f"   📊 Seasons stats: {pg.stats['seasons_inserted']} inserted/updated")

        # Final summary
        total_time = time.time() - start_time
        log("=" * 80)
        log("🎯 DATA INITIALIZATION COMPLETED SUCCESSFULLY!")
        log("=" * 80)
        log(f"📊 DETAILED SUMMARY:")
        log(f"   👥 People:")
        log(f"      - Inserted: {pg.stats['people_inserted']}")
        log(f"      - Updated: {pg.stats['people_updated']}")
        log(f"      - Total: {inserted_people}")
        log(f"   🎬 Movies:")
        log(f"      - Inserted: {pg.stats['movies_inserted']}")
        log(f"      - Updated: {pg.stats['movies_updated']}")
        log(f"      - Total: {inserted_movies}")
        log(f"   📺 Seasons:")
        log(f"      - Inserted/Updated: {pg.stats['seasons_inserted']}")
        log(f"      - Total: {inserted_seasons}")
        log(f"   🌍 Countries created: {pg.stats['countries_created']}")
        log(f"   🎭 Genres created: {pg.stats['genres_created']}")
        log(f"   🎬 Distributors created: {pg.stats['distributors_created']}")
        log(f"   🔗 Relationships:")
        log(f"      - Movie-Countries: {pg.stats['movie_countries_linked']}")
        log(f"      - Movie-Genres: {pg.stats['movie_genres_linked']}")
        log(f"      - Movie-People: {pg.stats['movie_people_linked']}")
        log(f"      - Movie-Facts: {pg.stats['movie_facts_inserted']}")
        log(f"      - Movie-Videos: {pg.stats['movie_videos_inserted']}")
        log(f"   ⏱️  Performance:")
        log(f"      - Total time: {total_time:.2f} seconds")
        log(f"      - Average speed: {((inserted_people + inserted_movies + inserted_seasons) / total_time):.2f} records/second")
        log(f"      - Batch size: {BATCH_SIZE}")
        log(f"      - Max workers: {MAX_WORKERS}")
        log("=" * 80)

    except Exception as e:
        pg.conn.rollback()
        log(f"❌ Error during seeding: {e}")
        raise
    finally:
        log("🧹 Cleaning up connections...")
        pg.close()
        if es:
            try:
                es.close()
                log("✅ Elasticsearch connection closed")
            except Exception:
                pass
        if redis_repo:
            try:
                redis_repo.close()
                log("✅ Redis connection closed")
            except Exception:
                pass
        try:
            mclient.close()
            log("✅ MongoDB connection closed")
        except Exception:
            pass
        log("🎉 All connections closed successfully")


if __name__ == "__main__":
    start_time = time.time()
    log("Starting data initialization service…")
    log(f"Performance settings: BATCH_SIZE={BATCH_SIZE}, MAX_WORKERS={MAX_WORKERS}, COMMIT_INTERVAL={COMMIT_INTERVAL}")
    log(f"Redis caching: {'enabled' if ENABLE_REDIS else 'disabled'}")
    seed_from_mongo()
    elapsed = time.time() - start_time
    log(f"Data initialization completed in {elapsed:.2f} seconds")


