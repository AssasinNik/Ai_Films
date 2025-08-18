-- =====================================================
-- PostgreSQL Schema for Movie Recommendation System
-- Based on Emotional Mood Analysis
-- =====================================================

-- Create database (run as superuser)
-- CREATE DATABASE movie_recommendation_db;
-- \c movie_recommendation_db;

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- =====================================================
-- REFERENCE TABLES
-- =====================================================

-- Countries table
CREATE TABLE countries (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    iso_code VARCHAR(3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Genres table
CREATE TABLE genres (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- People table (actors, directors, producers, etc.)
CREATE TABLE people (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    en_name VARCHAR(200),
    birth_date DATE,
    death_date DATE,
    birth_place VARCHAR(200),
    photo_url TEXT,
    biography TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Roles table
CREATE TABLE roles (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT
);

-- Distributors table
CREATE TABLE distributors (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    release_name VARCHAR(200),
    country_id INTEGER REFERENCES countries(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- MAIN MOVIES TABLE
-- =====================================================

CREATE TABLE movies (
    id BIGSERIAL PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    alternative_name VARCHAR(500),
    en_name VARCHAR(500),
    description TEXT,
    age_rating INTEGER,
    movie_length INTEGER, -- in minutes
    slogan TEXT,
    type VARCHAR(50), -- 'movie', 'tv-series', 'cartoon', etc.
    year INTEGER,
    premiere_world DATE,
    premiere_russia DATE,
    
    -- Ratings
    rating_kp DECIMAL(3,1),
    rating_imdb DECIMAL(3,1),
    rating_film_critics DECIMAL(3,1),
    rating_russian_film_critics DECIMAL(3,1),
    
    -- Vote counts
    votes_kp INTEGER,
    votes_imdb INTEGER,
    votes_film_critics INTEGER,
    votes_russian_film_critics INTEGER,
    
    -- Budget and box office
    budget_value BIGINT,
    budget_currency VARCHAR(10),
    fees_world_value BIGINT,
    fees_world_currency VARCHAR(10),
    fees_russia_value BIGINT,
    fees_russia_currency VARCHAR(10),
    fees_usa_value BIGINT,
    fees_usa_currency VARCHAR(10),
    
    -- Media resources
    poster_url TEXT,
    poster_preview_url TEXT,
    backdrop_url TEXT,
    backdrop_preview_url TEXT,
    
    -- External IDs
    external_id_imdb VARCHAR(20),
    external_id_tmdb INTEGER,
    external_id_kphd VARCHAR(50),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_deleted BOOLEAN DEFAULT FALSE,
    
    -- Constraints
    CONSTRAINT movies_year_check CHECK (year >= 1800 AND year <= 2100),
    CONSTRAINT movies_rating_kp_check CHECK (rating_kp >= 0 AND rating_kp <= 10),
    CONSTRAINT movies_rating_imdb_check CHECK (rating_imdb >= 0 AND rating_imdb <= 10),
    CONSTRAINT movies_age_rating_check CHECK (age_rating >= 0 AND age_rating <= 21)
);

-- =====================================================
-- RELATIONSHIP TABLES
-- =====================================================

-- Movie-Country relationships
CREATE TABLE movie_countries (
    movie_id BIGINT REFERENCES movies(id) ON DELETE CASCADE,
    country_id INTEGER REFERENCES countries(id),
    PRIMARY KEY (movie_id, country_id)
);

-- Movie-Genre relationships
CREATE TABLE movie_genres (
    movie_id BIGINT REFERENCES movies(id) ON DELETE CASCADE,
    genre_id INTEGER REFERENCES genres(id),
    PRIMARY KEY (movie_id, genre_id)
);

-- Movie-People relationships with roles
CREATE TABLE movie_people (
    id BIGSERIAL PRIMARY KEY,
    movie_id BIGINT REFERENCES movies(id) ON DELETE CASCADE,
    person_id BIGINT REFERENCES people(id),
    role_id INTEGER REFERENCES roles(id),
    character_name VARCHAR(200),
    order_index INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(movie_id, person_id, role_id, character_name)
);

-- Movie-Distributor relationships
CREATE TABLE movie_distributors (
    movie_id BIGINT REFERENCES movies(id) ON DELETE CASCADE,
    distributor_id INTEGER REFERENCES distributors(id),
    distribution_type VARCHAR(50), -- 'theatrical', 'digital', 'dvd', etc.
    release_date DATE,
    PRIMARY KEY (movie_id, distributor_id, distribution_type)
);

-- =====================================================
-- TV SERIES SPECIFIC TABLES
-- =====================================================

-- Seasons table
CREATE TABLE seasons (
    id BIGSERIAL PRIMARY KEY,
    movie_id BIGINT REFERENCES movies(id) ON DELETE CASCADE,
    season_number INTEGER NOT NULL,
    episodes_count INTEGER,
    air_date DATE,
    poster_url TEXT,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(movie_id, season_number)
);

-- Episodes table
CREATE TABLE episodes (
    id BIGSERIAL PRIMARY KEY,
    season_id BIGINT REFERENCES seasons(id) ON DELETE CASCADE,
    episode_number INTEGER NOT NULL,
    title VARCHAR(500),
    en_title VARCHAR(500),
    synopsis TEXT,
    air_date DATE,
    runtime INTEGER, -- in minutes
    still_url TEXT,
    still_preview_url TEXT,
    rating_kp DECIMAL(3,1),
    rating_imdb DECIMAL(3,1),
    votes_kp INTEGER,
    votes_imdb INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(season_id, episode_number),
    CONSTRAINT episodes_rating_kp_check CHECK (rating_kp >= 0 AND rating_kp <= 10),
    CONSTRAINT episodes_rating_imdb_check CHECK (rating_imdb >= 0 AND rating_imdb <= 10)
);

-- =====================================================
-- CONTENT TABLES
-- =====================================================

-- Reviews table
CREATE TABLE reviews (
    id BIGSERIAL PRIMARY KEY,
    movie_id BIGINT REFERENCES movies(id) ON DELETE CASCADE,
    author VARCHAR(200),
    review_type VARCHAR(50), -- 'positive', 'negative', 'neutral'
    review_date DATE,
    title VARCHAR(500),
    review_text TEXT,
    user_rating INTEGER CHECK (user_rating >= 1 AND user_rating <= 10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Movie facts table
CREATE TABLE movie_facts (
    id BIGSERIAL PRIMARY KEY,
    movie_id BIGINT REFERENCES movies(id) ON DELETE CASCADE,
    fact_text TEXT NOT NULL,
    fact_type VARCHAR(50), -- 'FACT', 'BLOOPER', 'QUOTE', etc.
    is_spoiler BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Movie videos table (trailers, teasers, etc.)
CREATE TABLE movie_videos (
    id BIGSERIAL PRIMARY KEY,
    movie_id BIGINT REFERENCES movies(id) ON DELETE CASCADE,
    video_url TEXT NOT NULL,
    video_name VARCHAR(500),
    video_site VARCHAR(50), -- 'youtube', 'vimeo', etc.
    video_type VARCHAR(50), -- 'trailer', 'teaser', 'clip', etc.
    video_quality VARCHAR(20), -- '480p', '720p', '1080p', etc.
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- EMOTIONAL ANALYSIS TABLES
-- =====================================================

-- Emotional tags for movies
CREATE TABLE movie_emotional_tags (
    id BIGSERIAL PRIMARY KEY,
    movie_id BIGINT REFERENCES movies(id) ON DELETE CASCADE,
    emotion VARCHAR(50) NOT NULL, -- 'happy', 'sad', 'exciting', 'calming', etc.
    intensity DECIMAL(3,2) CHECK (intensity >= 0 AND intensity <= 1),
    confidence DECIMAL(3,2) CHECK (confidence >= 0 AND confidence <= 1),
    source VARCHAR(50), -- 'manual', 'ai_analysis', 'user_feedback'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(movie_id, emotion)
);

-- Movie mood categories
CREATE TABLE movie_mood_categories (
    id BIGSERIAL PRIMARY KEY,
    movie_id BIGINT REFERENCES movies(id) ON DELETE CASCADE,
    mood_category VARCHAR(50) NOT NULL, -- 'feel_good', 'comfort', 'energizing', 'relaxing'
    weight DECIMAL(3,2) CHECK (weight >= 0 AND weight <= 1),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(movie_id, mood_category)
);

-- =====================================================
-- INDEXES FOR OPTIMIZATION
-- =====================================================

-- Movies table indexes
CREATE INDEX idx_movies_title_gin ON movies USING gin(to_tsvector('russian', title));
CREATE INDEX idx_movies_description_gin ON movies USING gin(to_tsvector('russian', description));
CREATE INDEX idx_movies_year ON movies(year);
CREATE INDEX idx_movies_type ON movies(type);
CREATE INDEX idx_movies_rating_kp ON movies(rating_kp DESC) WHERE rating_kp IS NOT NULL;
CREATE INDEX idx_movies_rating_imdb ON movies(rating_imdb DESC) WHERE rating_imdb IS NOT NULL;
CREATE INDEX idx_movies_external_imdb ON movies(external_id_imdb) WHERE external_id_imdb IS NOT NULL;
CREATE INDEX idx_movies_external_tmdb ON movies(external_id_tmdb) WHERE external_id_tmdb IS NOT NULL;
CREATE INDEX idx_movies_created_at ON movies(created_at DESC);
CREATE INDEX idx_movies_not_deleted ON movies(id) WHERE is_deleted = FALSE;

-- People table indexes
CREATE INDEX idx_people_name_gin ON people USING gin(to_tsvector('russian', name));
CREATE INDEX idx_people_en_name ON people(en_name) WHERE en_name IS NOT NULL;

-- Relationship table indexes
CREATE INDEX idx_movie_countries_country ON movie_countries(country_id);
CREATE INDEX idx_movie_genres_genre ON movie_genres(genre_id);
CREATE INDEX idx_movie_people_person ON movie_people(person_id);
CREATE INDEX idx_movie_people_role ON movie_people(role_id);
CREATE INDEX idx_movie_people_order ON movie_people(movie_id, order_index);

-- Content table indexes
CREATE INDEX idx_reviews_movie_id ON reviews(movie_id);
CREATE INDEX idx_reviews_type ON reviews(review_type);
CREATE INDEX idx_reviews_date ON reviews(review_date DESC);
CREATE INDEX idx_movie_facts_movie_id ON movie_facts(movie_id);
CREATE INDEX idx_movie_facts_type ON movie_facts(fact_type);
CREATE INDEX idx_movie_videos_movie_id ON movie_videos(movie_id);
CREATE INDEX idx_movie_videos_type ON movie_videos(video_type);

-- Emotional analysis indexes
CREATE INDEX idx_movie_emotional_tags_movie ON movie_emotional_tags(movie_id);
CREATE INDEX idx_movie_emotional_tags_emotion ON movie_emotional_tags(emotion);
CREATE INDEX idx_movie_emotional_tags_intensity ON movie_emotional_tags(intensity DESC);
CREATE INDEX idx_movie_mood_categories_movie ON movie_mood_categories(movie_id);
CREATE INDEX idx_movie_mood_categories_category ON movie_mood_categories(mood_category);

-- TV series indexes
CREATE INDEX idx_seasons_movie ON seasons(movie_id);
CREATE INDEX idx_episodes_season ON episodes(season_id);
CREATE INDEX idx_episodes_air_date ON episodes(air_date DESC);

-- =====================================================
-- TRIGGERS FOR AUTOMATIC UPDATES
-- =====================================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Triggers for updated_at
CREATE TRIGGER update_movies_updated_at BEFORE UPDATE ON movies
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_people_updated_at BEFORE UPDATE ON people
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- =====================================================
-- VIEWS FOR COMMON QUERIES
-- =====================================================

-- View for movies with all related data
CREATE VIEW movies_full AS
SELECT 
    m.*,
    COALESCE(
        json_agg(
            DISTINCT jsonb_build_object(
                'id', g.id,
                'name', g.name
            )
        ) FILTER (WHERE g.id IS NOT NULL), 
        '[]'::json
    ) as genres,
    COALESCE(
        json_agg(
            DISTINCT jsonb_build_object(
                'id', c.id,
                'name', c.name
            )
        ) FILTER (WHERE c.id IS NOT NULL), 
        '[]'::json
    ) as countries,
    COALESCE(
        json_agg(
            DISTINCT jsonb_build_object(
                'id', p.id,
                'name', p.name,
                'role', r.name,
                'character_name', mp.character_name
            )
        ) FILTER (WHERE p.id IS NOT NULL), 
        '[]'::json
    ) as people
FROM movies m
LEFT JOIN movie_genres mg ON m.id = mg.movie_id
LEFT JOIN genres g ON mg.genre_id = g.id
LEFT JOIN movie_countries mc ON m.id = mc.movie_id
LEFT JOIN countries c ON mc.country_id = c.id
LEFT JOIN movie_people mp ON m.id = mp.movie_id
LEFT JOIN people p ON mp.person_id = p.id
LEFT JOIN roles r ON mp.role_id = r.id
WHERE m.is_deleted = FALSE
GROUP BY m.id;

-- View for movie statistics
CREATE VIEW movie_statistics AS
SELECT 
    m.id,
    m.title,
    m.year,
    m.type,
    COALESCE(m.rating_kp, 0) as rating_kp,
    COALESCE(m.rating_imdb, 0) as rating_imdb,
    COALESCE(m.votes_kp, 0) as votes_kp,
    COALESCE(m.votes_imdb, 0) as votes_imdb,
    COUNT(DISTINCT r.id) as review_count,
    AVG(r.user_rating) as avg_user_rating,
    COUNT(DISTINCT mf.id) as fact_count,
    COUNT(DISTINCT mv.id) as video_count
FROM movies m
LEFT JOIN reviews r ON m.id = r.movie_id
LEFT JOIN movie_facts mf ON m.id = mf.movie_id
LEFT JOIN movie_videos mv ON m.id = mv.movie_id
WHERE m.is_deleted = FALSE
GROUP BY m.id, m.title, m.year, m.type, m.rating_kp, m.rating_imdb, m.votes_kp, m.votes_imdb;

-- =====================================================
-- INITIAL DATA INSERTION
-- =====================================================

-- Insert basic roles
INSERT INTO roles (name, description) VALUES
('actor', 'Актер'),
('director', 'Режиссер'),
('producer', 'Продюсер'),
('writer', 'Сценарист'),
('composer', 'Композитор'),
('cinematographer', 'Оператор'),
('editor', 'Монтажер'),
('production_designer', 'Художник-постановщик');

-- Insert basic countries
INSERT INTO countries (name, iso_code) VALUES
('США', 'USA'),
('Россия', 'RUS'),
('Великобритания', 'GBR'),
('Франция', 'FRA'),
('Германия', 'DEU'),
('Италия', 'ITA'),
('Испания', 'ESP'),
('Канада', 'CAN'),
('Австралия', 'AUS'),
('Япония', 'JPN'),
('Южная Корея', 'KOR'),
('Китай', 'CHN'),
('Индия', 'IND');

-- Insert basic genres
INSERT INTO genres (name, description) VALUES
('драма', 'Драматические фильмы'),
('комедия', 'Комедийные фильмы'),
('боевик', 'Боевики и экшн'),
('триллер', 'Триллеры и саспенс'),
('ужасы', 'Фильмы ужасов'),
('фантастика', 'Научная фантастика'),
('фэнтези', 'Фэнтези и магия'),
('романтика', 'Романтические фильмы'),
('приключения', 'Приключенческие фильмы'),
('криминал', 'Криминальные фильмы'),
('детектив', 'Детективы и расследования'),
('военный', 'Военные фильмы'),
('исторический', 'Исторические фильмы'),
('биография', 'Биографические фильмы'),
('документальный', 'Документальные фильмы'),
('мультфильм', 'Анимационные фильмы'),
('семейный', 'Семейные фильмы'),
('мюзикл', 'Мюзиклы'),
('спорт', 'Спортивные фильмы'),
('вестерн', 'Вестерны');

-- =====================================================
-- PERFORMANCE OPTIMIZATION SETTINGS
-- =====================================================

-- Analyze tables for better query planning
ANALYZE;

-- Create statistics for better query optimization
CREATE STATISTICS movies_genre_year_stats ON year, type FROM movies;
CREATE STATISTICS movies_rating_votes_stats ON rating_kp, votes_kp FROM movies;

COMMIT;

