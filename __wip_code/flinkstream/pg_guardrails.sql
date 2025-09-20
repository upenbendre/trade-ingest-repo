-- Example trades table (assumed)
CREATE TABLE IF NOT EXISTS trades (
    trade_id TEXT NOT NULL,
    version INT NOT NULL,
    counter_party_id TEXT,
    book_id TEXT,
    maturity_date DATE,
    created_date DATE,
    payload JSONB,
    expired_flag BOOLEAN DEFAULT FALSE,
    last_updated TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (trade_id, version)
);

-- Upsert function with guardrails:
CREATE OR REPLACE FUNCTION public.upsert_trade(
    p_trade_id TEXT,
    p_version INT,
    p_counter_party_id TEXT,
    p_book_id TEXT,
    p_maturity_date DATE,
    p_created_date DATE,
    p_payload JSONB
) RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    v_latest_version INT;
BEGIN
    -- Find latest active version for this trade_id
    SELECT max(version) INTO v_latest_version
    FROM trades
    WHERE trade_id = p_trade_id
      AND expired_flag = false;

    -- Rejection rule 1: incoming version lower than latest active
    IF v_latest_version IS NOT NULL AND p_version < v_latest_version THEN
        RETURN 'REJECTED: lower_version';
    END IF;

    -- Rejection rule 2: same version exists active but incoming maturity < today
    IF v_latest_version IS NOT NULL AND p_version = v_latest_version AND p_maturity_date < current_date THEN
        RETURN 'REJECTED: same_version_maturity_before_today';
    END IF;

    -- Upsert logic:
    -- Insert if not exists; if exists, update fields (we only update same PK (trade_id,version) row).
    INSERT INTO trades (trade_id, version, counter_party_id, book_id, maturity_date, created_date, payload, expired_flag, last_updated)
    VALUES (p_trade_id, p_version, p_counter_party_id, p_book_id, p_maturity_date, p_created_date, p_payload, false, now())
    ON CONFLICT (trade_id, version) DO UPDATE
    SET counter_party_id = EXCLUDED.counter_party_id,
        book_id = EXCLUDED.book_id,
        maturity_date = EXCLUDED.maturity_date,
        created_date = EXCLUDED.created_date,
        payload = EXCLUDED.payload,
        expired_flag = EXCLUDED.expired_flag,
        last_updated = now()
    -- Additional DB-side guard: do not let this update overwrite a newer active version
    WHERE (EXCLUDED.version >= trades.version) AND (trades.expired_flag = false);

    -- If insert succeeded, return inserted or updated:
    IF FOUND THEN
        RETURN 'OK';
    ELSE
        -- If ON CONFLICT WHERE prevented update (because it failed WHERE) then it's a potential conflicting condition:
        RETURN 'REJECTED: db_guardrail_prevented_update';
    END IF;
END;
$$;

'''
Usage: 
-- Use a prepared statement in the sink driver
SELECT public.upsert_trade($1, $2, $3, $4, $5, $6, $7);
-- If the function returns 'REJECTED: ...' treat as a reject and forward the original record to DLQ.
-- If it returns 'OK' / 'INSERTED' / 'UPDATED' treat as accepted.

'''