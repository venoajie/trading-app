-- init.sql
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

CREATE TABLE ohlc15_btc_perp_json (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    open_interest REAL,
    tick INTEGER GENERATED ALWAYS AS ((data->>'tick')::INTEGER) STORED
);

CREATE TABLE ohlc1_btc_perp_json (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    open_interest REAL,
    tick INTEGER GENERATED ALWAYS AS ((data->>'tick')::INTEGER) STORED
);


CREATE TABLE ohlc1_eth_perp_json (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    open_interest REAL,
    tick INTEGER GENERATED ALWAYS AS ((data->>'tick')::INTEGER) STORED
);

CREATE OR REPLACE FUNCTION get_arithmetic_value(
    p_item TEXT,
    p_operator TEXT DEFAULT 'MAX',
    p_table TEXT DEFAULT 'ohlc1_eth_perp_json'
) RETURNS FLOAT AS $$
DECLARE
    result_value FLOAT;
    query_text TEXT;
BEGIN
    -- Sanitize inputs to prevent SQL injection
    IF p_operator NOT IN ('MAX', 'MIN', 'AVG', 'SUM', 'COUNT') THEN
        RAISE EXCEPTION 'Invalid operator: %', p_operator;
    END IF;
    
    IF p_table !~ '^[a-z0-9_]+$' THEN
        RAISE EXCEPTION 'Invalid table name: %', p_table;
    END IF;
    
    IF p_item !~ '^[a-z0-9_]+$' THEN
        RAISE EXCEPTION 'Invalid item name: %', p_item;
    END IF;
    
    query_text := format('SELECT %s(%I) FROM %I', p_operator, p_item, p_table);
    
    EXECUTE query_text INTO result_value;
    
    RETURN result_value;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


-- Main partitioned orders table
CREATE TABLE orders (
    user_id TEXT,
    currency VARCHAR(5) NOT NULL,
    instrument_name TEXT NOT NULL,
    label TEXT,
    amount_dir NUMERIC,
    price NUMERIC,
    side VARCHAR(4),
    timestamp TIMESTAMPTZ,
    trade_id TEXT,
    order_id TEXT,
    is_open BOOLEAN DEFAULT TRUE,
    data JSONB NOT NULL,
    uid TEXT GENERATED ALWAYS AS (
        CASE 
            WHEN trade_id IS NOT NULL THEN 'trade_' || trade_id
            ELSE 'order_' || order_id
        END
    ) STORED,
    PRIMARY KEY (currency, instrument_name, uid)
) PARTITION BY LIST (currency);

-- Partitions for each currency
CREATE TABLE orders_btc PARTITION OF orders FOR VALUES IN ('BTC');
CREATE TABLE orders_eth PARTITION OF orders FOR VALUES IN ('ETH');
CREATE TABLE orders_usdc PARTITION OF orders FOR VALUES IN ('USDC');
CREATE TABLE orders_usdt PARTITION OF orders FOR VALUES IN ('USDT');

-- Generated columns for efficient querying
ALTER TABLE orders
    ADD COLUMN amount_dir_calc NUMERIC GENERATED ALWAYS AS (
        CASE 
            WHEN side = 'sell' THEN - (data->>'amount')::NUMERIC 
            ELSE (data->>'amount')::NUMERIC 
        END
    ) STORED;

-- view for active trades
CREATE VIEW v_trading_active AS
SELECT instrument_name, label, amount_dir_calc AS amount, 
       price, side, timestamp, trade_id, order_id
FROM orders
WHERE is_open = TRUE AND trade_id IS NOT NULL;

-- view for active orders
CREATE VIEW v_orders AS
SELECT instrument_name, label, amount_dir_calc AS amount, 
       price, side, timestamp, order_id
FROM orders
WHERE trade_id IS NULL;

-- Indexes for critical columns
CREATE INDEX idx_orders_uid ON orders (uid);
CREATE INDEX idx_orders_is_open ON orders (is_open) WHERE is_open = TRUE;
CREATE INDEX idx_orders_timestamp ON orders (timestamp);
CREATE INDEX idx_orders_instrument ON orders (instrument_name);
CREATE INDEX idx_orders_trade_id ON orders (trade_id);
CREATE INDEX idx_orders_order_id ON orders (order_id);

-- JSONB indexes for efficient querying
CREATE INDEX idx_orders_data_gin ON orders USING GIN (data);
CREATE INDEX idx_orders_data_trade_id ON orders ((data->>'trade_id'));
CREATE INDEX idx_orders_data_order_id ON orders ((data->>'order_id'));