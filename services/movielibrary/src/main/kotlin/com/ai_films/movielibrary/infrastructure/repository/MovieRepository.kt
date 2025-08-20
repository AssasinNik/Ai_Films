package com.ai_films.movielibrary.infrastructure.repository

import com.ai_films.movielibrary.domain.model.MediaItemCard
import com.ai_films.movielibrary.domain.model.MovieDetails
import com.ai_films.movielibrary.domain.model.PersonCredit
import com.ai_films.movielibrary.domain.model.SeasonDetails
import com.ai_films.movielibrary.domain.queries.MovieSearchFilters
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Repository
class MovieRepository(private val client: DatabaseClient) {

    fun searchByText(
        query: String,
        filters: MovieSearchFilters,
        page: Int,
        size: Int
    ): Mono<Pair<List<MediaItemCard>, Long>> {
        val offset = (page - 1) * size

        val conditions = mutableListOf<String>()
        val params = mutableMapOf<String, Any>()

        conditions += "m.is_deleted = FALSE"
        conditions += "(to_tsvector('russian', m.title) @@ plainto_tsquery('russian', :q) OR to_tsvector('russian', m.description) @@ plainto_tsquery('russian', :q) OR to_tsvector('russian', coalesce(m.alternative_name,'') || ' ' || coalesce(m.en_name,'')) @@ plainto_tsquery('russian', :q))"
        params["q"] = query

        applyCommonFilters(filters, conditions, params)

        val orderBy = orderClause(filters)

        val base = """
            FROM movies m
            LEFT JOIN movie_genres mg ON mg.movie_id = m.id
            LEFT JOIN genres g ON g.id = mg.genre_id
            LEFT JOIN movie_countries mc ON mc.movie_id = m.id
            LEFT JOIN countries c ON c.id = mc.country_id
            WHERE ${conditions.joinToString(" AND ")}
        """.trimIndent()

        val sql = """
            SELECT m.id,
                   m.title,
                   COALESCE(m.rating_kp, m.rating_imdb) as rating,
                   m.year,
                   m.description,
                   m.poster_preview_url as poster,
                   m.type,
                   COALESCE(json_agg(DISTINCT g.name) FILTER (WHERE g.id IS NOT NULL), '[]') as genres,
                   COALESCE(json_agg(DISTINCT c.name) FILTER (WHERE c.id IS NOT NULL), '[]') as countries
            $base
            GROUP BY m.id
            $orderBy
            LIMIT :limit OFFSET :offset
        """.trimIndent()

        val countSql = "SELECT COUNT(DISTINCT m.id) $base"

        val itemsStmt = client.sql(sql)
            .bind("q", params["q"]!!)
            .let { st ->
                var s = st
                params["type"]?.let { s = s.bind("type", it) }
                params["yearFrom"]?.let { s = s.bind("yearFrom", it) }
                params["yearTo"]?.let { s = s.bind("yearTo", it) }
                params["country"]?.let { s = s.bind("country", it) }
                params["minRating"]?.let { s = s.bind("minRating", it) }
                params["maxRating"]?.let { s = s.bind("maxRating", it) }
                params["ageRatingMax"]?.let { s = s.bind("ageRatingMax", it) }
                s
            }
            .bind("limit", size)
            .bind("offset", offset)
        val itemsMono = itemsStmt
            .map { row, _ ->
                MediaItemCard(
                    id = (row.get("id") as Number).toLong(),
                    title = row.get("title") as String?,
                    rating = (row.get("rating") as? Number)?.toDouble(),
                    year = (row.get("year") as? Number)?.toInt(),
                    genres = (row.get("genres") as? String)?.let { JsonArrayParser.parseStringArray(it) } ?: emptyList(),
                    description = row.get("description") as String?,
                    country = (row.get("countries") as? String)?.let { JsonArrayParser.parseStringArray(it) } ?: emptyList(),
                    posterUrl = row.get("poster") as String?,
                    type = row.get("type") as String? ?: "movie",
                )
            }
            .all()
            .collectList()

        val countStmt = client.sql(countSql)
            .bind("q", params["q"]!!)
            .let { st ->
                var s = st
                params["type"]?.let { s = s.bind("type", it) }
                params["yearFrom"]?.let { s = s.bind("yearFrom", it) }
                params["yearTo"]?.let { s = s.bind("yearTo", it) }
                params["country"]?.let { s = s.bind("country", it) }
                params["minRating"]?.let { s = s.bind("minRating", it) }
                params["maxRating"]?.let { s = s.bind("maxRating", it) }
                params["ageRatingMax"]?.let { s = s.bind("ageRatingMax", it) }
                s
            }
        val countMono = countStmt.map { row, _ -> (row.get(0) as Number).toLong() }.one()

        return itemsMono.zipWith(countMono).map { it.t1 to it.t2 }
    }

    fun searchByGenres(
        genres: List<String>,
        filters: MovieSearchFilters,
        page: Int,
        size: Int
    ): Mono<Pair<List<MediaItemCard>, Long>> {
        val offset = (page - 1) * size
        val conditions = mutableListOf<String>("m.is_deleted = FALSE")
        val params = mutableMapOf<String, Any>()

        applyCommonFilters(filters, conditions, params)

        if (genres.isNotEmpty()) {
            conditions += "g.name = ANY(:genres)"
            params["genres"] = genres.toTypedArray()
        }

        val base = """
            FROM movies m
            LEFT JOIN movie_genres mg ON mg.movie_id = m.id
            LEFT JOIN genres g ON g.id = mg.genre_id
            LEFT JOIN movie_countries mc ON mc.movie_id = m.id
            LEFT JOIN countries c ON c.id = mc.country_id
            WHERE ${conditions.joinToString(" AND ")}
        """.trimIndent()

        val orderBy = "ORDER BY matched_genres DESC, COALESCE(m.rating_kp, m.rating_imdb) DESC, m.year DESC"

        val sql = """
            WITH base AS (
                SELECT m.id,
                       m.title,
                       COALESCE(m.rating_kp, m.rating_imdb) as rating,
                       m.year,
                       m.description,
                       m.poster_preview_url as poster,
                       m.type,
                       COALESCE(json_agg(DISTINCT g.name) FILTER (WHERE g.id IS NOT NULL), '[]') as genres,
                       COALESCE(json_agg(DISTINCT c.name) FILTER (WHERE c.id IS NOT NULL), '[]') as countries,
                       COUNT(DISTINCT CASE WHEN g.name = ANY(:genres) THEN g.id END) as matched_genres
                $base
                GROUP BY m.id
            )
            SELECT * FROM base
            $orderBy
            LIMIT :limit OFFSET :offset
        """.trimIndent()

        val countSql = "SELECT COUNT(DISTINCT m.id) $base"

        val itemsStmtG = client.sql(sql)
            .let { st ->
                var s = st
                params["type"]?.let { s = s.bind("type", it) }
                params["yearFrom"]?.let { s = s.bind("yearFrom", it) }
                params["yearTo"]?.let { s = s.bind("yearTo", it) }
                params["country"]?.let { s = s.bind("country", it) }
                params["minRating"]?.let { s = s.bind("minRating", it) }
                params["maxRating"]?.let { s = s.bind("maxRating", it) }
                params["ageRatingMax"]?.let { s = s.bind("ageRatingMax", it) }
                params["genres"]?.let { s = s.bind("genres", it) }
                s
            }
            .bind("limit", size)
            .bind("offset", offset)
        val itemsMono = itemsStmtG
            .map { row, _ ->
                MediaItemCard(
                    id = (row.get("id") as Number).toLong(),
                    title = row.get("title") as String?,
                    rating = (row.get("rating") as? Number)?.toDouble(),
                    year = (row.get("year") as? Number)?.toInt(),
                    genres = (row.get("genres") as? String)?.let { JsonArrayParser.parseStringArray(it) } ?: emptyList(),
                    description = row.get("description") as String?,
                    country = (row.get("countries") as? String)?.let { JsonArrayParser.parseStringArray(it) } ?: emptyList(),
                    posterUrl = row.get("poster") as String?,
                    type = row.get("type") as String? ?: "movie",
                )
            }
            .all()
            .collectList()
        val countStmtG = client.sql(countSql)
            .let { st ->
                var s = st
                params["type"]?.let { s = s.bind("type", it) }
                params["yearFrom"]?.let { s = s.bind("yearFrom", it) }
                params["yearTo"]?.let { s = s.bind("yearTo", it) }
                params["country"]?.let { s = s.bind("country", it) }
                params["minRating"]?.let { s = s.bind("minRating", it) }
                params["maxRating"]?.let { s = s.bind("maxRating", it) }
                params["ageRatingMax"]?.let { s = s.bind("ageRatingMax", it) }
                params["genres"]?.let { s = s.bind("genres", it) }
                s
            }
        val countMono = countStmtG.map { row, _ -> (row.get(0) as Number).toLong() }.one()
        return itemsMono.zipWith(countMono).map { it.t1 to it.t2 }
    }

    fun topNewReleases(limit: Int = 10): Flux<MediaItemCard> {
        val sql = """
            SELECT m.id,
                   m.title,
                   COALESCE(m.rating_kp, m.rating_imdb) as rating,
                   m.year,
                   m.description,
                   m.poster_preview_url as poster,
                   m.type,
                   COALESCE(json_agg(DISTINCT g.name) FILTER (WHERE g.id IS NOT NULL), '[]') as genres,
                   COALESCE(json_agg(DISTINCT c.name) FILTER (WHERE c.id IS NOT NULL), '[]') as countries
            FROM movies m
            LEFT JOIN movie_genres mg ON mg.movie_id = m.id
            LEFT JOIN genres g ON g.id = mg.genre_id
            LEFT JOIN movie_countries mc ON mc.movie_id = m.id
            LEFT JOIN countries c ON c.id = mc.country_id
            WHERE m.is_deleted = FALSE AND (m.rating_kp IS NOT NULL OR m.rating_imdb IS NOT NULL)
            GROUP BY m.id
            ORDER BY m.created_at DESC, COALESCE(m.rating_kp, m.rating_imdb) DESC
            LIMIT :limit
        """.trimIndent()

        return client.sql(sql)
            .bind("limit", limit)
            .map { row, _ ->
                MediaItemCard(
                    id = (row.get("id") as Number).toLong(),
                    title = row.get("title") as String?,
                    rating = (row.get("rating") as? Number)?.toDouble(),
                    year = (row.get("year") as? Number)?.toInt(),
                    genres = (row.get("genres") as? String)?.let { JsonArrayParser.parseStringArray(it) } ?: emptyList(),
                    description = row.get("description") as String?,
                    country = (row.get("countries") as? String)?.let { JsonArrayParser.parseStringArray(it) } ?: emptyList(),
                    posterUrl = row.get("poster") as String?,
                    type = row.get("type") as String? ?: "movie",
                )
            }
            .all()
    }

    fun details(movieId: Long): Mono<MovieDetails> {
        val mainSql = """
            SELECT m.*, 
                   COALESCE(json_agg(DISTINCT g.name) FILTER (WHERE g.id IS NOT NULL), '[]') AS genres,
                   COALESCE(json_agg(DISTINCT c.name) FILTER (WHERE c.id IS NOT NULL), '[]') AS countries
            FROM movies m
            LEFT JOIN movie_genres mg ON mg.movie_id = m.id
            LEFT JOIN genres g ON g.id = mg.genre_id
            LEFT JOIN movie_countries mc ON mc.movie_id = m.id
            LEFT JOIN countries c ON c.id = mc.country_id
            WHERE m.id = :id AND m.is_deleted = FALSE
            GROUP BY m.id
        """.trimIndent()

        val peopleSql = """
            SELECT p.id, p.name, r.name as role, mp.character_name, p.photo_url
            FROM movie_people mp
            LEFT JOIN people p ON p.id = mp.person_id
            LEFT JOIN roles r ON r.id = mp.role_id
            WHERE mp.movie_id = :id
            ORDER BY mp.order_index ASC
        """.trimIndent()

        val seasonsSql = """
            SELECT id, season_number, episodes_count, air_date, poster_url, description
            FROM seasons
            WHERE movie_id = :id
            ORDER BY season_number ASC
        """.trimIndent()

        val mainMono = client.sql(mainSql)
            .bind("id", movieId)
            .map { row, _ ->
                Pair(
                    row, // keep row for later field access
                    MovieDetails(
                        id = movieId,
                        title = row.get("title") as String?,
                        alternativeName = row.get("alternative_name") as String?,
                        enName = row.get("en_name") as String?,
                        description = row.get("description") as String?,
                        ageRating = (row.get("age_rating") as? Number)?.toInt(),
                        year = (row.get("year") as? Number)?.toInt(),
                        type = row.get("type") as String? ?: "movie",
                        posterUrl = row.get("poster_url") as String?,
                        backdropUrl = row.get("backdrop_url") as String?,
                        ratingKp = (row.get("rating_kp") as? Number)?.toDouble(),
                        ratingImdb = (row.get("rating_imdb") as? Number)?.toDouble(),
                        genres = (row.get("genres") as? String)?.let { JsonArrayParser.parseStringArray(it) } ?: emptyList(),
                        countries = (row.get("countries") as? String)?.let { JsonArrayParser.parseStringArray(it) } ?: emptyList(),
                        people = emptyList(),
                        seasons = emptyList(),
                    )
                )
            }
            .one()

        val peopleFlux = client.sql(peopleSql)
            .bind("id", movieId)
            .map { row, _ ->
                PersonCredit(
                    id = (row.get("id") as? Number)?.toLong(),
                    name = row.get("name") as String?,
                    role = row.get("role") as String?,
                    characterName = row.get("character_name") as String?,
                    photoUrl = row.get("photo_url") as String?,
                )
            }
            .all()
            .collectList()

        val seasonsFlux = client.sql(seasonsSql)
            .bind("id", movieId)
            .map { row, _ ->
                SeasonDetails(
                    id = (row.get("id") as Number).toLong(),
                    seasonNumber = (row.get("season_number") as Number).toInt(),
                    episodesCount = (row.get("episodes_count") as? Number)?.toInt(),
                    airDate = row.get("air_date")?.toString(),
                    posterUrl = row.get("poster_url") as String?,
                    description = row.get("description") as String?,
                )
            }
            .all()
            .collectList()

        return mainMono.flatMap { pair ->
            val base = pair.second
            Mono.zip(peopleFlux, seasonsFlux).map { tuple ->
                base.copy(people = tuple.t1, seasons = tuple.t2)
            }
        }
    }

    private fun applyCommonFilters(
        filters: MovieSearchFilters,
        conditions: MutableList<String>,
        params: MutableMap<String, Any>
    ) {
        filters.type?.let { conditions += "m.type = :type"; params["type"] = it }
        filters.yearFrom?.let { conditions += "m.year >= :yearFrom"; params["yearFrom"] = it }
        filters.yearTo?.let { conditions += "m.year <= :yearTo"; params["yearTo"] = it }
        filters.country?.let { conditions += "EXISTS (SELECT 1 FROM movie_countries mc JOIN countries c ON c.id = mc.country_id WHERE mc.movie_id = m.id AND c.name = :country)"; params["country"] = it }
        filters.minRating?.let { conditions += "COALESCE(m.rating_kp, m.rating_imdb) >= :minRating"; params["minRating"] = it }
        filters.maxRating?.let { conditions += "COALESCE(m.rating_kp, m.rating_imdb) <= :maxRating"; params["maxRating"] = it }
        filters.ageRatingMax?.let { conditions += "m.age_rating <= :ageRatingMax"; params["ageRatingMax"] = it }
    }

    private fun orderClause(filters: MovieSearchFilters): String {
        val dir = if (filters.sort.direction.equals("asc", ignoreCase = true)) "ASC" else "DESC"
        return when (filters.sort.orderBy.lowercase()) {
            "year" -> "ORDER BY m.year $dir, COALESCE(m.rating_kp, m.rating_imdb) DESC"
            "title" -> "ORDER BY m.title $dir"
            else -> "ORDER BY COALESCE(m.rating_kp, m.rating_imdb) $dir, m.year DESC"
        }
    }
}

object JsonArrayParser {
    // Postgres json_agg returns text in R2DBC as String, parse simple ["a","b"] into list
    fun parseStringArray(json: String): List<String> {
        val trimmed = json.trim()
        if (!trimmed.startsWith("[") || !trimmed.endsWith("]")) return emptyList()
        val inner = trimmed.substring(1, trimmed.length - 1).trim()
        if (inner.isEmpty()) return emptyList()
        val parts = mutableListOf<String>()
        var i = 0
        while (i < inner.length) {
            if (inner[i] == '"') {
                val sb = StringBuilder()
                i++
                while (i < inner.length && inner[i] != '"') {
                    val ch = inner[i]
                    if (ch == '\\' && i + 1 < inner.length) {
                        val next = inner[i + 1]
                        sb.append(next)
                        i += 2
                        continue
                    }
                    sb.append(ch)
                    i++
                }
                parts.add(sb.toString())
                // move past closing quote and optional comma
                while (i < inner.length && inner[i] != ',') i++
                if (i < inner.length && inner[i] == ',') i++
            } else {
                i++
            }
        }
        return parts
    }
}


