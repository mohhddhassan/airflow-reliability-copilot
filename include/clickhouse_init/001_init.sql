-- Seed schema + demo data for Airflow Reliability Copilot.
-- Mounted into the ClickHouse container's docker-entrypoint-initdb.d/ so it
-- runs automatically the first time the container starts.

CREATE DATABASE IF NOT EXISTS reliability_demo;

USE reliability_demo;

-- The "real" table the orders pipeline loads into.
CREATE TABLE IF NOT EXISTS orders
(
    order_id      UInt64,
    customer_id   UInt64,
    order_total   Decimal(10, 2),
    created_at    DateTime,
    status        String
)
ENGINE = MergeTree
ORDER BY order_id;

-- Replica / replication health, checked by the investigation DAG.
CREATE TABLE IF NOT EXISTS replica_health
(
    table_name  String,
    is_healthy  UInt8,
    lag_seconds UInt32,
    notes       String,
    checked_at  DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY checked_at;

-- Row-count snapshots, used to compute drift between pipeline runs.
CREATE TABLE IF NOT EXISTS row_count_snapshots
(
    table_name  String,
    snapshot_ts DateTime DEFAULT now(),
    row_count   UInt64
)
ENGINE = MergeTree
ORDER BY snapshot_ts;

-- Recent query errors, surfaced to the LLM as evidence.
CREATE TABLE IF NOT EXISTS query_errors
(
    table_name    String,
    occurred_at   DateTime DEFAULT now(),
    error_message String
)
ENGINE = MergeTree
ORDER BY occurred_at;

-- Audit trail: one row per investigation, written by reliability_investigation
-- regardless of whether the human approved or rejected remediation.
CREATE TABLE IF NOT EXISTS incidents
(
    dag_run_id         String,
    source_dag_id      String,
    probable_cause     String,
    confidence         String,
    evidence           String,  -- JSON-encoded list of evidence strings
    recommended_action String,
    decision           String,  -- 'approved' | 'rejected'
    decided_by         String,
    created_at         DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY created_at;

-- --- Seed data ---------------------------------------------------------
-- Intentionally unhealthy on first boot, so `orders_pipeline`'s pre-publish
-- health check fails and the investigation/HITL/remediation demo has
-- something real to react to.

INSERT INTO orders (order_id, customer_id, order_total, created_at, status)
SELECT
    number AS order_id,
    number % 500 AS customer_id,
    round(10 + rand() % 40000 / 100.0, 2) AS order_total,
    now() - toIntervalMinute(number % 1440) AS created_at,
    ['pending', 'paid', 'shipped'][(number % 3) + 1] AS status
FROM numbers(5000);

INSERT INTO replica_health (table_name, is_healthy, lag_seconds, notes, checked_at)
VALUES
    ('orders', 0, 812, 'replica-2 has not acknowledged writes since 08:14 UTC - suspected network partition', now() - toIntervalMinute(3)),
    ('orders', 0, 640, 'replication lag climbing steadily over the last 15 minutes', now() - toIntervalMinute(9)),
    ('orders', 1, 4, 'healthy', now() - toIntervalHour(1));

INSERT INTO row_count_snapshots (table_name, snapshot_ts, row_count)
VALUES
    ('orders', now() - toIntervalMinute(2), 5000),
    ('orders', now() - toIntervalHour(1), 4870),
    ('orders', now() - toIntervalHour(2), 4820);

INSERT INTO query_errors (table_name, occurred_at, error_message)
VALUES
    ('orders', now() - toIntervalMinute(4), 'DB::Exception: Timeout exceeded while waiting for replica acknowledgment (orders, replica-2)'),
    ('orders', now() - toIntervalMinute(6), 'DB::Exception: Connection reset by peer while replicating part 202609_1_1_0');
