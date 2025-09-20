-- trades_range_partition.sql
CREATE SCHEMA IF NOT EXISTS tradeschema;

CREATE TABLE IF NOT EXISTS tradeschema.trades (
    trade_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    counter_party_id TEXT,
    book_id TEXT,
    maturity_date DATE NOT NULL,
    created_date DATE NOT NULL,
    payload JSONB,
    expired_flag BOOLEAN NOT NULL DEFAULT FALSE,
    ingest_ts TIMESTAMP WITH TIME ZONE DEFAULT now(),
    CONSTRAINT pk_trade_tradeid_version PRIMARY KEY (trade_id, version)
) PARTITION BY RANGE (maturity_date);

-- Example monthly partitions for 2024-2026 (create as necessary)
CREATE TABLE IF NOT EXISTS tradeschema.trades_2024_01 PARTITION OF tradeschema.trades
  FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
-- Repeat for months or implement script to create partitions ahead of time.

CREATE INDEX IF NOT EXISTS idx_trades_active_maturity ON tradeschema.trades (maturity_date) WHERE expired_flag = FALSE;
CREATE INDEX IF NOT EXISTS idx_trades_active_tradeid ON tradeschema.trades (trade_id) WHERE expired_flag = FALSE;
ALTER TABLE tradeschema.trades SET (fillfactor = 90);

-- reuse trigger function from above (same enforce_trade_guardrails)
-- create trigger similar to above on the parent table
