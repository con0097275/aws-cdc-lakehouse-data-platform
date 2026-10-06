-- Realtime serving layer -- STREAMING_RT.
--
-- Two data tables and one view, exactly the shape the reference implementation uses
-- (plstream v9, `flow3_pl_ceo_v2`). The whole architecture turns on one sentence from that
-- codebase's own design note:
--
--     THE STREAM DOES NOT FIX ITS OWN MISTAKES.
--
-- It writes what it managed to enrich, and records what it could not into two POINTER
-- tables. A slower correction pass reads those pointers, rebuilds the dimensions from a
-- fresher source, and repairs BASE. That division is what lets the fast path stay fast
-- without lying: an unenriched row is still published (so a report never loses an account)
-- and is simultaneously flagged (so it never stays wrong).
--
--   rt_account_base     settled + corrected. Written by EOD and AUTOCORRECT.
--   rt_account_stream   intraday, seconds old, dims may be NULL. Written by STREAM.
--   v_rt_account        the merge. STREAM wins for any key newer than the watermark.
--
-- WHY A WATERMARK AND NOT "PREFER STREAM ALWAYS": after EOD rebuilds BASE for the closed
-- day, yesterday's STREAM rows are stale by construction -- they were the fast, possibly
-- dim-incomplete view of a day that has since been settled exactly. The watermark is the
-- timestamp BASE is authoritative up to; only STREAM rows *after* it may override.

CREATE TABLE IF NOT EXISTS glue_catalog.kafka_dev_lab_dev_stream.rt_account_base (
    account_id      BIGINT,
    customer_id     BIGINT,
    balance         DECIMAL(18,2),
    currency        STRING,
    product_code    STRING,
    account_status  STRING,
    -- dims resolved from the CUSTOMER stream. NULL here is meaningful: it is the state
    -- AUTOCORRECT source 2 hunts for.
    segment_code    STRING,
    branch_id       BIGINT,
    source_ts       TIMESTAMP,      -- event time from the source commit, never wall clock
    dim_complete    BOOLEAN,
    built_by        STRING,         -- EOD | AUTOCORRECT -- who last wrote this row
    built_at        TIMESTAMP
) USING iceberg
  PARTITIONED BY (bucket(16, account_id))
  TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS glue_catalog.kafka_dev_lab_dev_stream.rt_account_stream (
    account_id      BIGINT,
    customer_id     BIGINT,
    balance         DECIMAL(18,2),
    currency        STRING,
    product_code    STRING,
    account_status  STRING,
    segment_code    STRING,
    branch_id       BIGINT,
    source_ts       TIMESTAMP,
    dim_complete    BOOLEAN,        -- false => a pointer row exists for this account
    batch_id        BIGINT,
    written_at      TIMESTAMP
) USING iceberg
  PARTITIONED BY (bucket(16, account_id))
  TBLPROPERTIES ('format-version' = '2');

-- ---------------------------------------------------------------------------------------
-- POINTER TABLE 1 -- "the stream could not resolve this account's dims"
--
-- Written by the STREAM app when a row times out waiting for its dimension. Read by
-- AUTOCORRECT (`resolved_ts IS NULL`). Cleared by AUTOCORRECT when it repairs the row.
--
-- resolved_ts is NULLABLE ON PURPOSE and is the entire protocol: NULL means outstanding.
-- A boolean `is_resolved` would lose WHEN it was fixed, which is the only way to measure
-- how long the fast path was publishing an incomplete number.
-- ---------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS glue_catalog.kafka_dev_lab_dev_ops.rt_pending_dim (
    account_id      BIGINT,
    customer_id     BIGINT,
    missing_dims    STRING,         -- which join failed, e.g. 'customer'
    first_seen_ts   TIMESTAMP,
    retry_count     INT,
    resolved_ts     TIMESTAMP,      -- NULL => still outstanding
    resolved_by     STRING
) USING iceberg
  TBLPROPERTIES ('format-version' = '2');

-- ---------------------------------------------------------------------------------------
-- POINTER TABLE 2 -- "a dimension VALUE changed, so rows already written are now stale"
--
-- The first pointer table catches rows that never got their dims. This one catches the
-- opposite and subtler case: the row was enriched correctly, and then the dimension itself
-- changed. Nothing about the fact row is wrong, yet every row carrying the old dim value is
-- now out of date, and no amount of re-reading the FACT stream would reveal it.
-- ---------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS glue_catalog.kafka_dev_lab_dev_ops.rt_dim_change_audit (
    customer_id     BIGINT,
    changed_field   STRING,
    old_value       STRING,
    new_value       STRING,
    detected_ts     TIMESTAMP,
    processed_ts    TIMESTAMP       -- NULL => AUTOCORRECT has not yet fanned this out
) USING iceberg
  TBLPROPERTIES ('format-version' = '2');

-- The point BASE is authoritative up to. Written by EOD only.
CREATE TABLE IF NOT EXISTS glue_catalog.kafka_dev_lab_dev_ops.rt_base_watermark (
    layer           STRING,
    watermark_ts    TIMESTAMP,
    written_by      STRING,
    written_at      TIMESTAMP
) USING iceberg
  TBLPROPERTIES ('format-version' = '2');

-- Datamart output. `append_history` semantics: every polling cycle appends, so the series
-- of cycles is itself the evidence that the long-running app was alive and progressing.
CREATE TABLE IF NOT EXISTS glue_catalog.kafka_dev_lab_dev_ops.rt_datamart_metrics (
    section         STRING,
    grain           STRING,
    dim_value       STRING,
    metric_name     STRING,
    metric_value    DECIMAL(24,4),
    row_count       BIGINT,
    cycle_id        BIGINT,
    build_ts        TIMESTAMP
) USING iceberg
  TBLPROPERTIES ('format-version' = '2');
