CREATE SCHEMA If NOT EXISTS raw;

CREATE SCHEMA IF NOT EXISTS core;

CREATE SCHEMA IF NOT EXISTS marts;

-- raw: история запусков пайплайна
CREATE TABLE IF NOT EXISTS raw.pipeline_runs (
    run_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status VARCHAR(10) NOT NULL,
    error_message TEXT,
    CONSTRAINT chk_pipeline_runs_status CHECK (
            'running',
            'success',
            'failed',
            'partial'
        )
    CONSTRAINT chk_pipeline_runs_finished CHECK (
        (
            status = 'running'
            AND finished_at IS NULL
        )
        OR (
            status IN ('success', 'failed')
            AND finished_at IS NOT NULL
        )
    )
);

-- raw: исходные ответы API
CREATE TABLE IF NOT EXISTS raw.raw_api_responses (
    response_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES raw.pipeline_runs (run_id),
    source TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    page_number INT,
    page_size INT,
    count_result INT,
    payload JSONB
);

-- core: справочник игр
CREATE TABLE IF NOT EXISTS core.games (
    game_id INT PRIMARY KEY,
    title TEXT NOT NULL,
    internal_name TEXT,
    metacritic_score INT,
    steam_rating_text TEXT,
    steam_rating_percent INT,
    steam_rating_count INT,
    steam_app_id INT,
    release_date TIMESTAMPTZ,
    thumb TEXT,
    CONSTRAINT chk_games_metacritic CHECK (
        metacritic_score IS NULL
        OR metacritic_score BETWEEN 0 AND 100
    ),
    CONSTRAINT chk_games_steam_rating_percent CHECK (
        steam_rating_percent IS NULL
        OR steam_rating_percent BETWEEN 0 AND 100
    ),
    CONSTRAINT chk_games_steam_rating_count CHECK (
        steam_rating_count IS NULL
        OR steam_rating_count >= 0
    )
);

-- core: справочник магазинов
CREATE TABLE IF NOT EXISTS core.stores (
    store_id INT PRIMARY KEY,
    store_name TEXT NOT NULL,
    is_active bool NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_change_unix BIGINT 
);

-- core: актуальные предложения
CREATE TABLE IF NOT EXISTS core.deals_current (
    deal_id TEXT PRIMARY KEY,
    game_id INT NOT NULL REFERENCES core.games (game_id),
    store_id INT NOT NULL REFERENCES core.stores (store_id),
    sale_price NUMERIC(10, 2) NOT NULL,
    normal_price NUMERIC(10, 2) NOT NULl,
    savings NUMERIC(5, 2) NOT NULL,
    is_on_sale BOOLEAN,
    source_last_change TIMESTAMPTZ NOT NULL,
    discount_calculable bool NOT NULL,
    deal_rating NUMERIC(3, 1),
    CONSTRAINT uq_deals_current_game_store UNIQUE (game_id, store_id),
    CONSTRAINT chk_deals_current_sale_price CHECK (sale_price >= 0),
    CONSTRAINT chk_deals_current_normal_price CHECK (normal_price >= 0),
    CONSTRAINT chk_deals_current_savings CHECK (
        savings IS NULL
        OR savings BETWEEN 0 AND 100
    )
);

-- core: история состояний цен.
CREATE TABLE IF NOT EXISTS core.deals_price_history (
    history_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    game_id INT NOT NULL REFERENCES core.games (game_id),
    store_id INT NOT NULL REFERENCES core.stores (store_id),
    deal_id TEXT NOT NULL,
    sale_price NUMERIC(10, 2) NOT NULL,
    normal_price NUMERIC(10, 2) NOT NULL,
    savings NUMERIC(5, 2),
    is_on_sale BOOLEAN,
    source_last_change TIMESTAMPTZ,
    observed_at TIMESTAMPTZ NOT NULL,
    archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_id BIGINT NOT NULL REFERENCES raw.pipeline_runs (run_id),
    CONSTRAINT chk_history_sale_price CHECK (sale_price >= 0),
    CONSTRAINT chk_history_normal_price CHECK (normal_price >= 0),
    CONSTRAINT chk_history_savings CHECK (
        savings IS NULL
        OR savings BETWEEN 0 AND 100
    )
);

CREATE INDEX IF NOT EXISTS idx_deals_current_game_store ON core.deals_current (game_id, store_id);

CREATE INDEX IF NOT EXISTS idx_deals_current_sale_price ON core.deals_current (sale_price);

CREATE INDEX IF NOT EXISTS idx_deals_history_game_store_time ON core.deals_price_history (
    game_id,
    store_id,
    archived_at
);

CREATE INDEX IF NOT EXISTS idx_raw_responses_run_id ON raw.raw_api_responses (run_id);