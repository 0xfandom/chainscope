-- Day-partition maintenance for the raw event tables.
--
-- Partitions do not appear on their own. Something has to create tomorrow's
-- partition before tomorrow's first event arrives, or the insert fails (see the
-- note in 0004 about deliberately having no DEFAULT partition).
--
-- ensure_day_partitions() is that something. It is idempotent, so it is safe to
-- call on every startup and again from a scheduled task; running it twice
-- creates nothing the second time.

CREATE OR REPLACE FUNCTION ensure_day_partitions(
    p_days_ahead  INTEGER DEFAULT 7,
    p_days_behind INTEGER DEFAULT 1
) RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_parent   TEXT;
    v_day      DATE;
    v_name     TEXT;
    v_created  INTEGER := 0;
BEGIN
    FOREACH v_parent IN ARRAY ARRAY['swaps', 'liq_events'] LOOP
        v_day := CURRENT_DATE - p_days_behind;

        WHILE v_day <= CURRENT_DATE + p_days_ahead LOOP
            v_name := format('%s_%s', v_parent, to_char(v_day, 'YYYYMMDD'));

            IF to_regclass(format('public.%I', v_name)) IS NULL THEN
                -- FROM is inclusive, TO is exclusive, so consecutive days abut
                -- exactly with no gap and no overlap.
                EXECUTE format(
                    'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                    v_name, v_parent, v_day, v_day + 1
                );
                v_created := v_created + 1;
            END IF;

            v_day := v_day + 1;
        END LOOP;
    END LOOP;

    RETURN v_created;
END;
$$;

COMMENT ON FUNCTION ensure_day_partitions IS
    'Idempotently creates day partitions for swaps and liq_events. Returns how many were created.';

-- Bootstrap: without this the very first insert after migrating would fail.
SELECT ensure_day_partitions();
