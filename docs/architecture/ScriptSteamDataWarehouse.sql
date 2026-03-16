-- ====================================================
-- Script: Steam Data Warehouse for SQL Server
-- Description: Creates database and all tables with
-- constraints, unique keys, checks, indexes,
-- and audit columns (created_at, updated_at, etl_run_id,
-- is_active, valid_from, valid_to, is_current) as required.
-- ====================================================

CREATE DATABASE Steam_BI;
GO

USE Steam_BI;
GO

-- ====================================================
-- ETL Run Log table (must exist before any reference)
-- ====================================================
CREATE TABLE etl_run_log (
    run_id        BIGINT        IDENTITY(1,1) PRIMARY KEY,
    script_name   NVARCHAR(255) NOT NULL,
    started_at    DATETIME2     NOT NULL DEFAULT GETDATE(),
    finished_at   DATETIME2,
    status        NVARCHAR(20)  NOT NULL DEFAULT 'running',
    rows_inserted INTEGER       DEFAULT 0,
    rows_updated  INTEGER       DEFAULT 0,
    rows_skipped  INTEGER       DEFAULT 0,
    error_message NVARCHAR(MAX),
    CONSTRAINT chk_etl_status CHECK (status IN ('running', 'success', 'failed'))
);
GO

-- ====================================================
-- Static catalog dimensions (with audit columns)
-- ====================================================
CREATE TABLE dim_country (
    country_key  SMALLINT      IDENTITY(1,1) PRIMARY KEY,
    iso_code     NVARCHAR(10)  NOT NULL UNIQUE,
    country_name NVARCHAR(100) NOT NULL,
    created_at   DATETIME2     DEFAULT GETDATE(),
    updated_at   DATETIME2     DEFAULT GETDATE(),
    etl_run_id   BIGINT        REFERENCES etl_run_log(run_id)
);

CREATE TABLE dim_currency (
    currency_key    SMALLINT     IDENTITY(1,1) PRIMARY KEY,
    currency_code   NVARCHAR(3)  NOT NULL UNIQUE,
    currency_name   NVARCHAR(50) NOT NULL,
    currency_symbol NVARCHAR(10),
    minor_unit      SMALLINT,
    created_at      DATETIME2    DEFAULT GETDATE(),
    updated_at      DATETIME2    DEFAULT GETDATE(),
    etl_run_id      BIGINT       REFERENCES etl_run_log(run_id)
);

CREATE TABLE dim_language (
    language_key   SMALLINT      IDENTITY(1,1) PRIMARY KEY,
    iso_code       NVARCHAR(10)  NOT NULL UNIQUE,
    language_name  NVARCHAR(50)  NOT NULL,
    native_name    NVARCHAR(100),
    steam_api_name NVARCHAR(50) NULL,
    created_at     DATETIME2     DEFAULT GETDATE(),
    updated_at     DATETIME2     DEFAULT GETDATE(),
    etl_run_id     BIGINT        REFERENCES etl_run_log(run_id)
);

CREATE TABLE dim_date (
    date_key       INTEGER       PRIMARY KEY,
    full_date      DATE          NOT NULL,
    day_of_month   SMALLINT      NOT NULL,
    month_number   SMALLINT      NOT NULL,
    month_name     NVARCHAR(20)  NOT NULL,
    quarter_number SMALLINT      NOT NULL,
    year_number    SMALLINT      NOT NULL,
    day_of_week    SMALLINT      NOT NULL,
    day_name       NVARCHAR(20)  NOT NULL,
    etl_run_id     BIGINT        REFERENCES etl_run_log(run_id)
);

CREATE TABLE dim_time_bucket (
    time_bucket_key SMALLINT     IDENTITY(1,1) PRIMARY KEY,
    bucket_name     NVARCHAR(50) NOT NULL,
    created_at      DATETIME2    DEFAULT GETDATE(),
    updated_at      DATETIME2    DEFAULT GETDATE(),
    etl_run_id      BIGINT       REFERENCES etl_run_log(run_id)
);

-- ====================================================
-- Simple dimensions (with audit columns + is_active)
-- ====================================================
CREATE TABLE dim_genre (
    genre_key         BIGINT       IDENTITY(1,1) PRIMARY KEY,
    genre_id          INTEGER      NOT NULL UNIQUE,
    genre_description NVARCHAR(100) NOT NULL,
    created_at        DATETIME2    DEFAULT GETDATE(),
    updated_at        DATETIME2    DEFAULT GETDATE(),
    etl_run_id        BIGINT       REFERENCES etl_run_log(run_id),
    is_active         BIT          NOT NULL DEFAULT 1
);

CREATE TABLE dim_category (
    category_key         BIGINT       IDENTITY(1,1) PRIMARY KEY,
    category_id          INTEGER      NOT NULL UNIQUE,
    category_description NVARCHAR(150) NOT NULL,
    created_at           DATETIME2    DEFAULT GETDATE(),
    updated_at           DATETIME2    DEFAULT GETDATE(),
    etl_run_id           BIGINT       REFERENCES etl_run_log(run_id),
    is_active            BIT          NOT NULL DEFAULT 1
);

CREATE TABLE dim_developer (
    developer_key  BIGINT        IDENTITY(1,1) PRIMARY KEY,
    developer_name NVARCHAR(255) NOT NULL UNIQUE,
    created_at     DATETIME2     DEFAULT GETDATE(),
    updated_at     DATETIME2     DEFAULT GETDATE(),
    etl_run_id     BIGINT        REFERENCES etl_run_log(run_id),
    is_active      BIT           NOT NULL DEFAULT 1
);

CREATE TABLE dim_publisher (
    publisher_key  BIGINT        IDENTITY(1,1) PRIMARY KEY,
    publisher_name NVARCHAR(255) NOT NULL UNIQUE,
    created_at     DATETIME2     DEFAULT GETDATE(),
    updated_at     DATETIME2     DEFAULT GETDATE(),
    etl_run_id     BIGINT        REFERENCES etl_run_log(run_id),
    is_active      BIT           NOT NULL DEFAULT 1
);

-- ====================================================
-- Game dimension (SCD Type 2) with audit and SCD columns
-- ====================================================
CREATE TABLE dim_game (
    game_key             BIGINT        IDENTITY(1,1) PRIMARY KEY,
    appid                INTEGER       NOT NULL,
    game_name            NVARCHAR(255) NOT NULL,
    game_type            NVARCHAR(50),
    required_age         INTEGER,
    is_free              BIT,
    controller_support   NVARCHAR(50),
    website              NVARCHAR(500),
    release_date         DATE,
    coming_soon          BIT,
    recommendations_total INTEGER,
    achievements_total   INTEGER,
    metacritic_score     INTEGER       CHECK (metacritic_score BETWEEN 0 AND 100),
    platform_windows     BIT,
    platform_mac         BIT,
    platform_linux       BIT,
    last_modified_ts     DATETIME2,
    price_change_number  BIGINT,
    created_at           DATETIME2     DEFAULT GETDATE(),
    updated_at           DATETIME2     DEFAULT GETDATE(),
    etl_run_id           BIGINT        REFERENCES etl_run_log(run_id),
    valid_from           DATETIME2     NOT NULL DEFAULT GETDATE(),
    valid_to             DATETIME2,
    is_current           BIT           NOT NULL DEFAULT 1,
    is_active            BIT           NOT NULL DEFAULT 1
);

-- ====================================================
-- User dimension (SCD Type 2) with audit and SCD columns
-- ====================================================
CREATE TABLE dim_user (
    user_key                    BIGINT        IDENTITY(1,1) PRIMARY KEY,
    steamid_hash                NVARCHAR(255) NOT NULL,
    visibility_state            SMALLINT,
    profile_state               SMALLINT,
    persona_state               SMALLINT,
    country_key                 SMALLINT      REFERENCES dim_country(country_key),
    account_created_date        DATE,
    account_created_year        SMALLINT,
    account_age_band            NVARCHAR(20),
    last_logoff_date            DATE,
    last_logoff_time_bucket_key SMALLINT      REFERENCES dim_time_bucket(time_bucket_key),
    created_at                  DATETIME2     DEFAULT GETDATE(),
    updated_at                  DATETIME2     DEFAULT GETDATE(),
    etl_run_id                  BIGINT        REFERENCES etl_run_log(run_id),
    valid_from                  DATETIME2     NOT NULL DEFAULT GETDATE(),
    valid_to                    DATETIME2,
    is_current                  BIT           NOT NULL DEFAULT 1,
    is_active                   BIT           NOT NULL DEFAULT 1
);

-- ====================================================
-- Achievement dimension (simple dimension with is_active)
-- ====================================================
CREATE TABLE dim_achievement (
    achievement_key          BIGINT         IDENTITY(1,1) PRIMARY KEY,
    game_key                 BIGINT         NOT NULL REFERENCES dim_game(game_key),
    achievement_api_name     NVARCHAR(255)  NOT NULL,
    achievement_display_name NVARCHAR(255)  NOT NULL,
    achievement_description  NVARCHAR(1000),
    is_hidden                BIT,
    default_value            INTEGER,
    icon_url                 NVARCHAR(1000),
    icon_gray_url            NVARCHAR(1000),
    created_at               DATETIME2      DEFAULT GETDATE(),
    updated_at               DATETIME2      DEFAULT GETDATE(),
    etl_run_id               BIGINT         REFERENCES etl_run_log(run_id),
    is_active                BIT            NOT NULL DEFAULT 1
);

-- ====================================================
-- Bridge tables (many-to-many relationships)
-- ====================================================
CREATE TABLE bridge_game_genre (
    game_key  BIGINT NOT NULL REFERENCES dim_game(game_key),
    genre_key BIGINT NOT NULL REFERENCES dim_genre(genre_key),
    PRIMARY KEY (game_key, genre_key)
);

CREATE TABLE bridge_game_category (
    game_key     BIGINT NOT NULL REFERENCES dim_game(game_key),
    category_key BIGINT NOT NULL REFERENCES dim_category(category_key),
    PRIMARY KEY (game_key, category_key)
);

CREATE TABLE bridge_game_developer (
    game_key      BIGINT NOT NULL REFERENCES dim_game(game_key),
    developer_key BIGINT NOT NULL REFERENCES dim_developer(developer_key),
    PRIMARY KEY (game_key, developer_key)
);

CREATE TABLE bridge_game_publisher (
    game_key      BIGINT NOT NULL REFERENCES dim_game(game_key),
    publisher_key BIGINT NOT NULL REFERENCES dim_publisher(publisher_key),
    PRIMARY KEY (game_key, publisher_key)
);

CREATE TABLE bridge_game_language (
    game_key      BIGINT   NOT NULL REFERENCES dim_game(game_key),
    language_key  SMALLINT NOT NULL REFERENCES dim_language(language_key),
    has_interface BIT,
    has_audio     BIT,
    has_subtitles BIT,
    PRIMARY KEY (game_key, language_key)
);

-- ====================================================
-- Fact tables (with audit columns: created_at, etl_run_id)
-- ====================================================
CREATE TABLE fact_user_owned_game (
    user_key                     BIGINT   NOT NULL REFERENCES dim_user(user_key),
    game_key                     BIGINT   NOT NULL REFERENCES dim_game(game_key),
    date_key                     INTEGER  NOT NULL REFERENCES dim_date(date_key),
    playtime_forever_min         INTEGER,
    playtime_windows_forever_min INTEGER,
    playtime_mac_forever_min     INTEGER,
    playtime_linux_forever_min   INTEGER,
    playtime_deck_forever_min    INTEGER,
    rtime_last_played_date       DATE,
    rtime_last_played_bucket_key SMALLINT REFERENCES dim_time_bucket(time_bucket_key),
    has_visible_stats            BIT,
    has_leaderboards             BIT,
    has_workshop                 BIT,
    has_market                   BIT,
    has_dlc                      BIT,
    playtime_disconnected_min    INTEGER,
    created_at                   DATETIME2 DEFAULT GETDATE(),
    etl_run_id                   BIGINT    REFERENCES etl_run_log(run_id),
    PRIMARY KEY (user_key, game_key, date_key)
);

CREATE TABLE fact_user_recent_play (
    user_key                     BIGINT  NOT NULL REFERENCES dim_user(user_key),
    game_key                     BIGINT  NOT NULL REFERENCES dim_game(game_key),
    date_key                     INTEGER NOT NULL REFERENCES dim_date(date_key),
    playtime_2weeks_min          INTEGER,
    playtime_forever_min         INTEGER,
    playtime_windows_forever_min INTEGER,
    playtime_mac_forever_min     INTEGER,
    playtime_linux_forever_min   INTEGER,
    playtime_deck_forever_min    INTEGER,
    created_at                   DATETIME2 DEFAULT GETDATE(),
    etl_run_id                   BIGINT    REFERENCES etl_run_log(run_id),
    PRIMARY KEY (user_key, game_key, date_key)
);

CREATE TABLE fact_user_library_snapshot (
    user_key   BIGINT  NOT NULL REFERENCES dim_user(user_key),
    date_key   INTEGER NOT NULL REFERENCES dim_date(date_key),
    game_count INTEGER CHECK (game_count >= 0),
    created_at DATETIME2 DEFAULT GETDATE(),
    etl_run_id BIGINT    REFERENCES etl_run_log(run_id),
    PRIMARY KEY (user_key, date_key)
);

CREATE TABLE fact_achievement_global (
    achievement_key      BIGINT          NOT NULL REFERENCES dim_achievement(achievement_key),
    game_key             BIGINT          NOT NULL REFERENCES dim_game(game_key),
    date_key             INTEGER         NOT NULL REFERENCES dim_date(date_key),
    global_unlock_percent DECIMAL(5,2)   CHECK (global_unlock_percent BETWEEN 0 AND 100),
    created_at           DATETIME2       DEFAULT GETDATE(),
    etl_run_id           BIGINT          REFERENCES etl_run_log(run_id),
    PRIMARY KEY (achievement_key, date_key)
);

CREATE TABLE fact_game_achievement_summary (
    game_key                  BIGINT      NOT NULL REFERENCES dim_game(game_key),
    date_key                  INTEGER     NOT NULL REFERENCES dim_date(date_key),
    achievement_count_total   INTEGER,
    most_common_achievement_name NVARCHAR(MAX),
    most_common_percent       DECIMAL(5,2) CHECK (most_common_percent  BETWEEN 0 AND 100),
    rarest_achievement_name   NVARCHAR(MAX),
    rarest_percent            DECIMAL(5,2) CHECK (rarest_percent        BETWEEN 0 AND 100),
    closest_25_name           NVARCHAR(MAX),
    closest_25_percent        DECIMAL(5,2) CHECK (closest_25_percent    BETWEEN 0 AND 100),
    closest_50_name           NVARCHAR(MAX),
    closest_50_percent        DECIMAL(5,2) CHECK (closest_50_percent    BETWEEN 0 AND 100),
    closest_75_name           NVARCHAR(MAX),
    closest_75_percent        DECIMAL(5,2) CHECK (closest_75_percent    BETWEEN 0 AND 100),
    share_under_5_percent     DECIMAL(5,2) CHECK (share_under_5_percent  BETWEEN 0 AND 100),
    share_5_to_25_percent     DECIMAL(5,2) CHECK (share_5_to_25_percent  BETWEEN 0 AND 100),
    share_25_to_50_percent    DECIMAL(5,2) CHECK (share_25_to_50_percent BETWEEN 0 AND 100),
    share_50_to_75_percent    DECIMAL(5,2) CHECK (share_50_to_75_percent BETWEEN 0 AND 100),
    share_over_75_percent     DECIMAL(5,2) CHECK (share_over_75_percent  BETWEEN 0 AND 100),
    created_at                DATETIME2    DEFAULT GETDATE(),
    etl_run_id                BIGINT       REFERENCES etl_run_log(run_id),
    PRIMARY KEY (game_key, date_key)
);

CREATE TABLE fact_concurrent_players_snapshot (
    game_key             BIGINT    NOT NULL REFERENCES dim_game(game_key),
    captured_at          DATETIME2 NOT NULL,
    date_key             INTEGER   NOT NULL REFERENCES dim_date(date_key),
    current_player_count INTEGER   CHECK (current_player_count >= 0),
    created_at           DATETIME2 DEFAULT GETDATE(),
    etl_run_id           BIGINT    REFERENCES etl_run_log(run_id),
    PRIMARY KEY (game_key, captured_at)
);

CREATE TABLE fact_news_events (
    news_event_key BIGINT        IDENTITY(1,1) PRIMARY KEY,
    game_key       BIGINT        NOT NULL REFERENCES dim_game(game_key),
    news_gid       NVARCHAR(100),
    date_published DATETIME2,
    date_key       INTEGER       REFERENCES dim_date(date_key),
    title          NVARCHAR(500),
    feed_label     NVARCHAR(100),
    url            NVARCHAR(1000),
    author         NVARCHAR(255),
    contents_short NVARCHAR(2000),
    created_at     DATETIME2     DEFAULT GETDATE(),
    etl_run_id     BIGINT        REFERENCES etl_run_log(run_id)
);

CREATE TABLE fact_game_price_snapshot (
    game_key        BIGINT    NOT NULL REFERENCES dim_game(game_key),
    country_key     SMALLINT  NOT NULL REFERENCES dim_country(country_key),
    currency_key    SMALLINT  NOT NULL REFERENCES dim_currency(currency_key),
    date_key        INTEGER   NOT NULL REFERENCES dim_date(date_key),
    captured_at     DATETIME2 NOT NULL,
    initial_price   INTEGER,
    final_price     INTEGER,
    discount_percent INTEGER  CHECK (discount_percent BETWEEN 0 AND 100),
    created_at      DATETIME2 DEFAULT GETDATE(),
    etl_run_id      BIGINT    REFERENCES etl_run_log(run_id),
    PRIMARY KEY (game_key, country_key, currency_key, captured_at)
);

CREATE TABLE fact_game_price_period (
    game_key         BIGINT   NOT NULL REFERENCES dim_game(game_key),
    country_key      SMALLINT NOT NULL REFERENCES dim_country(country_key),
    currency_key     SMALLINT NOT NULL REFERENCES dim_currency(currency_key),
    valid_from_date  DATE     NOT NULL,
    valid_to_date    DATE,
    initial_price    INTEGER  CHECK (initial_price   >= 0),
    final_price      INTEGER  CHECK (final_price     >= 0),
    discount_percent INTEGER  CHECK (discount_percent BETWEEN 0 AND 100),
    created_at       DATETIME2 DEFAULT GETDATE(),
    etl_run_id       BIGINT    REFERENCES etl_run_log(run_id),
    PRIMARY KEY (game_key, country_key, currency_key, valid_from_date)
);

CREATE TABLE fact_game_review_summary (
    game_key         BIGINT   NOT NULL REFERENCES dim_game(game_key),
    date_key         INTEGER  NOT NULL REFERENCES dim_date(date_key),
    review_score     INTEGER,
    review_score_desc NVARCHAR(50),
    total_positive   INTEGER  CHECK (total_positive >= 0),
    total_negative   INTEGER  CHECK (total_negative >= 0),
    total_reviews    INTEGER  CHECK (total_reviews  >= 0),
    created_at       DATETIME2 DEFAULT GETDATE(),
    etl_run_id       BIGINT    REFERENCES etl_run_log(run_id),
    PRIMARY KEY (game_key, date_key)
);

CREATE TABLE fact_game_review_detail (
    recommendation_id              NVARCHAR(50)   PRIMARY KEY,
    game_key                       BIGINT         NOT NULL REFERENCES dim_game(game_key),
    created_date_key               INTEGER        REFERENCES dim_date(date_key),
    updated_date_key               INTEGER        REFERENCES dim_date(date_key),
    language_key                   SMALLINT       REFERENCES dim_language(language_key),
    review_text                    NVARCHAR(MAX),
    voted_up                       BIT,
    votes_up                       INTEGER        CHECK (votes_up                       >= 0),
    votes_funny                    INTEGER        CHECK (votes_funny                    >= 0),
    weighted_vote_score            DECIMAL(12,9),
    comment_count                  INTEGER        CHECK (comment_count                  >= 0),
    steam_purchase                 BIT,
    received_for_free              BIT,
    refunded                       BIT,
    written_during_early_access    BIT,
    primarily_steam_deck           BIT,
    author_playtime_forever        INTEGER        CHECK (author_playtime_forever        >= 0),
    author_playtime_last_two_weeks INTEGER        CHECK (author_playtime_last_two_weeks >= 0),
    author_playtime_at_review      INTEGER        CHECK (author_playtime_at_review      >= 0),
    author_last_played             DATETIME2,
    created_at                     DATETIME2      DEFAULT GETDATE(),
    etl_run_id                     BIGINT         REFERENCES etl_run_log(run_id)
);

-- ====================================================
-- Indexes for performance (foreign key columns)
-- ====================================================
CREATE INDEX idx_fact_user_owned_game_game_key
    ON fact_user_owned_game(game_key);

CREATE INDEX idx_fact_user_owned_game_date_key
    ON fact_user_owned_game(date_key);

CREATE INDEX idx_fact_user_recent_play_game_key
    ON fact_user_recent_play(game_key);

CREATE INDEX idx_fact_concurrent_players_snapshot_date_key
    ON fact_concurrent_players_snapshot(date_key);

CREATE INDEX idx_fact_game_price_snapshot_country_key
    ON fact_game_price_snapshot(country_key);

CREATE INDEX idx_fact_game_price_snapshot_currency_key
    ON fact_game_price_snapshot(currency_key);

CREATE INDEX idx_fact_game_review_detail_game_key
    ON fact_game_review_detail(game_key);

CREATE INDEX idx_dim_game_updated_at 
    ON dim_game(updated_at);
    
CREATE INDEX idx_dim_game_is_current
    ON dim_game(is_current);
    
CREATE INDEX idx_dim_user_updated_at 
    ON dim_user(updated_at);
    
CREATE INDEX idx_dim_user_is_current 
    ON dim_user(is_current);
    
CREATE INDEX idx_etl_run_log_started_at 
    ON etl_run_log(started_at);
    
CREATE INDEX idx_etl_run_log_status    
    ON etl_run_log(status);

CREATE UNIQUE INDEX uix_dim_game_appid_current 
    ON dim_game(appid) 
    WHERE is_current = 1;

GO

PRINT 'Steam Data Warehouse schema created successfully with audit columns.';