-- init.sql

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Create tables with JSON support
-- Main partitioned table
CREATE TABLE orders (
    currency VARCHAR(5) NOT NULL,
    instrument_name TEXT,
    label TEXT,
    amount_dir NUMERIC,
    price NUMERIC,
    side VARCHAR(4),
    timestamp TIMESTAMPTZ,
    trade_id TEXT PRIMARY KEY,
    is_open BOOLEAN
) PARTITION BY LIST (currency);

-- Partitions for each currency
CREATE TABLE orders_btc PARTITION OF my_trades_all_json
    FOR VALUES IN ('BTC');

CREATE TABLE orders_eth PARTITION OF my_trades_all_json
    FOR VALUES IN ('ETH');

CREATE TABLE orders_usdc PARTITION OF my_trades_all_json
    FOR VALUES IN ('USDC');

CREATE TABLE orders_usdt PARTITION OF my_trades_all_json
    FOR VALUES IN ('USDT');

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
CREATE VIEW v_trading_all_active AS
SELECT instrument_name, label, amount_dir AS amount, 
       price, side, timestamp, trade_id
FROM my_trades_all_json
WHERE is_open = true;
CREATE TABLE ohlc15_btc_perp_json (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    open_interest REAL,
    tick INTEGER GENERATED ALWAYS AS ((data->>'tick')::INTEGER) STORED
);

-- Add unique constraints and proper indexing
ALTER TABLE my_trades_all_btc_json 
  ADD CONSTRAINT unique_trade_id UNIQUE (trade_id);

CREATE INDEX idx_my_trades_timestamp ON my_trades_all_btc_json(timestamp);
CREATE INDEX idx_ohlc_tick ON ohlc15_btc_perp_json(tick);

-- Add foreign key relationship
ALTER TABLE orders_json
  ADD CONSTRAINT fk_trade_id 
  FOREIGN KEY (trade_id) 
  REFERENCES my_trades_all_btc_json(trade_id) ON DELETE CASCADE;

-- JSONB path indexing for frequent queries
CREATE INDEX idx_trade_data ON my_trades_all_btc_json 
  USING GIN ((data->'trades'));

-- Partial index for open trades
CREATE INDEX idx_open_trades ON my_trades_all_btc_json (is_open)
  WHERE is_open = 1;

-- Create indexes for performance (ONLY NEW INDEXES)
CREATE INDEX idx_my_trades_data_gin ON my_trades_all_btc_json USING GIN (data);

CREATE INDEX idx_open_trades ON orders (is_open)
    WHERE is_open = true;
CREATE INDEX idx_trade_id ON orders (trade_id);