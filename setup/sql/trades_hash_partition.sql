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
) PARTITION BY HASH (trade_id);

DO $$
BEGIN
  FOR i IN 0..31 LOOP
    EXECUTE format('CREATE TABLE IF NOT EXISTS tradeschema.trades_p%1$s PARTITION OF tradeschema.trades FOR VALUES WITH (MODULUS 32, REMAINDER %1$s);', i);
    -- set fillfactor on the leaf partition
    EXECUTE format('ALTER TABLE tradeschema.trades_p%1$s SET (fillfactor = 90);', i);
  END LOOP;
END$$;

CREATE INDEX IF NOT EXISTS idx_trades_active_maturity ON tradeschema.trades (maturity_date) WHERE expired_flag = FALSE;
CREATE INDEX IF NOT EXISTS idx_trades_active_tradeid ON tradeschema.trades (trade_id) WHERE expired_flag = FALSE;

CREATE OR REPLACE FUNCTION tradeschema.enforce_trade_guardrails()
RETURNS TRIGGER AS $$
DECLARE
    larger_active_count INT;
    existing_version_exists INT;
BEGIN
    IF (TG_OP = 'INSERT' OR TG_OP = 'UPDATE') AND (NEW.expired_flag = FALSE) THEN
        SELECT count(*) INTO larger_active_count
        FROM tradeschema.trades t
        WHERE t.trade_id = NEW.trade_id
          AND t.expired_flag = FALSE
          AND t.version > NEW.version;

        IF larger_active_count > 0 THEN
            RAISE EXCEPTION 'REJECTED_BY_DB_GUARDRAIL: active higher version exists for trade_id=%, incoming_version=%', NEW.trade_id, NEW.version;
        END IF;

        IF NEW.maturity_date < current_date THEN
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