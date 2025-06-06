-- init.sql

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Create tables with JSON support
CREATE TABLE my_trades_all_btc_json (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    instrument_name TEXT GENERATED ALWAYS AS (data->>'instrument_name') STORED,
    label TEXT GENERATED ALWAYS AS (data->>'label') STORED,
    trade_id TEXT GENERATED ALWAYS AS (data->>'trade_id') STORED,
    order_id TEXT GENERATED ALWAYS AS (data->>'order_id') STORED,
    amount_dir REAL GENERATED ALWAYS AS (
        CASE 
            WHEN data->>'direction' = 'sell' THEN - (data->>'amount')::REAL
            ELSE (data->>'amount')::REAL
        END
    ) STORED,
    price REAL GENERATED ALWAYS AS ((data->>'price')::REAL) STORED,
    side TEXT GENERATED ALWAYS AS (data->>'direction') STORED,
    timestamp BIGINT GENERATED ALWAYS AS ((data->>'timestamp')::BIGINT) STORED,
    user_seq INTEGER GENERATED ALWAYS AS ((data->>'user_seq')::INTEGER) STORED,
    is_open INTEGER NOT NULL DEFAULT 1 CHECK(is_open IN (0,1))
);

CREATE TABLE ohlc15_btc_perp_json (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    open_interest REAL,
    tick INTEGER GENERATED ALWAYS AS ((data->>'tick')::INTEGER) STORED
);

-- Create indexes for performance
CREATE INDEX idx_my_trades_timestamp ON my_trades_all_btc_json(timestamp);
CREATE INDEX idx_ohlc_tick ON ohlc15_btc_perp_json(tick);
CREATE INDEX idx_my_trades_data_gin ON my_trades_all_btc_json USING GIN (data);