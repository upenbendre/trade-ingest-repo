-- trades_hash_partition.sql
CREATE SCHEMA IF NOT EXISTS tradeschema;

-- Main table (parent for partitioning)
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
) PARTITION BY HASH (trade_id);

-- Create N hash partitions (choose number balanced with Postgres cluster)
-- Example: 32 partitions (tweak to your environment)
DO $$
BEGIN
  FOR i IN 0..31 LOOP
    EXECUTE format('
      CREATE TABLE IF NOT EXISTS tradeschema.trades_p%1$s PARTITION OF tradeschema.trades
      FOR VALUES WITH (MODULUS 32, REMAINDER %1$s);
    ', i);
  END LOOP;
END$$;

-- Partial index on maturity_date for active rows only (speeds active-read queries)
CREATE INDEX IF NOT EXISTS idx_trades_active_maturity ON tradeschema.trades (maturity_date)
  WHERE expired_flag = FALSE;

-- Index to speed reads filtering by trade_id + not expired (maybe used often)
CREATE INDEX IF NOT EXISTS idx_trades_active_tradeid ON tradeschema.trades (trade_id)
  WHERE expired_flag = FALSE;

-- Fillfactor to reduce page bloat
ALTER TABLE tradeschema.trades SET (fillfactor = 90);

-- ---------- DB-LEVEL GUARDRAIL via trigger ----------
-- 1) Reject insert/update if there exists an active row for same trade_id with version > NEW.version.
-- 2) Reject update to an existing active record (same trade_id+version) if NEW.maturity_date < current_date (and it's active).

CREATE OR REPLACE FUNCTION tradeschema.enforce_trade_guardrails()
RETURNS TRIGGER AS $$
DECLARE
    larger_active_count INT;
    existing_version_exists INT;
BEGIN
    -- If the row is being inserted or updated to be active (expired_flag = FALSE)
    IF (TG_OP = 'INSERT' OR TG_OP = 'UPDATE') AND (NEW.expired_flag = FALSE) THEN
        -- Rule 1: If there's any active row for same trade_id with version > NEW.version, reject.
        SELECT count(*) INTO larger_active_count
        FROM tradeschema.trades t
        WHERE t.trade_id = NEW.trade_id
          AND t.expired_flag = FALSE
          AND t.version > NEW.version;

        IF larger_active_count > 0 THEN
            RAISE EXCEPTION 'REJECTED_BY_DB_GUARDRAIL: active higher version exists for trade_id=%, incoming_version=%', NEW.trade_id, NEW.version;
        END IF;

        -- Rule 2: If inserting/updating same trade_id+version to active BUT its maturity_date < today -> reject.
        IF NEW.maturity_date < current_date THEN
            -- If it's the same version of an active record (or inserting same pair), reject.
            SELECT count(*) INTO existing_version_exists
            FROM tradeschema.trades t
            WHERE t.trade_id = NEW.trade_id
              AND t.version = NEW.version
              AND t.expired_flag = FALSE;

            IF existing_version_exists > 0 THEN
                RAISE EXCEPTION 'REJECTED_BY_DB_GUARDRAIL: inserting/updating active trade with maturity before today for trade_id=% version=%', NEW.trade_id, NEW.version;
            END IF;
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_enforce_trade_guardrails ON tradeschema.trades;
CREATE TRIGGER trg_enforce_trade_guardrails
BEFORE INSERT OR UPDATE ON tradeschema.trades
FOR EACH ROW EXECUTE FUNCTION tradeschema.enforce_trade_guardrails();
