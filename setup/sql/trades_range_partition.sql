--- This is an alternate way of declaring the schema 
-- sql/trades_range_partition.sql -- optional variant

CREATE SCHEMA IF NOT EXISTS tradeschema_range;

CREATE TABLE IF NOT EXISTS tradeschema_range.trades (
    trade_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    counter_party_id TEXT,
    book_id TEXT,
    maturity_date DATE NOT NULL,
    created_date DATE NOT NULL,
    payload JSONB,
    expired_flag BOOLEAN NOT NULL DEFAULT FALSE,
    ingest_ts TIMESTAMP WITH TIME ZONE DEFAULT now(),
    -- NOTE: include the partition key column (maturity_date) in the PK
    CONSTRAINT pk_trade_tradeid_version PRIMARY KEY (trade_id, version, maturity_date)
) PARTITION BY RANGE (maturity_date);

-- create indexes on the parent (they will be propagated to partitions on supported PG versions)
CREATE INDEX IF NOT EXISTS idx_trades_active_maturity ON tradeschema_range.trades (maturity_date) WHERE expired_flag = FALSE;
CREATE INDEX IF NOT EXISTS idx_trades_active_tradeid ON tradeschema_range.trades (trade_id) WHERE expired_flag = FALSE;

-- DO NOT SET storage parameters (fillfactor) on the parent; set them on partitions instead.
