"""
Thin ClickHouse client helper shared by the DAGs. Dependency-light and
framework-agnostic so it's easy to unit test outside of Airflow.
"""
from __future__ import annotations

import json
import os
import random
from dataclasses import dataclass
from typing import Any


def _get_client():
    """Lazy import so DAG parsing doesn't require clickhouse-connect."""
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database=os.environ.get("CLICKHOUSE_DATABASE", "reliability_demo"),
    )


@dataclass
class TableHealth:
    table: str
    replica_healthy: bool
    replication_lag_seconds: int
    row_count: int
    row_count_delta_pct: float
    recent_query_errors: int
    notes: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "replica_healthy": self.replica_healthy,
            "replication_lag_seconds": self.replication_lag_seconds,
            "row_count": self.row_count,
            "row_count_delta_pct": self.row_count_delta_pct,
            "recent_query_errors": self.recent_query_errors,
            "notes": self.notes,
        }


def check_table_health(table: str) -> dict[str, Any]:
    """Replica health, row-count drift, and recent errors for one table.
    Expects the seed schema from `clickhouse_init/001_init.sql`.
    """
    client = _get_client()

    health_row = client.query(
        "SELECT is_healthy, lag_seconds, notes FROM replica_health "
        "WHERE table_name = {table:String} "
        "ORDER BY checked_at DESC LIMIT 1",
        parameters={"table": table},
    ).result_rows

    is_healthy, lag_seconds, notes = (
        health_row[0] if health_row else (True, 0, "no health rows found")
    )

    snapshots = client.query(
        "SELECT row_count FROM row_count_snapshots "
        "WHERE table_name = {table:String} "
        "ORDER BY snapshot_ts DESC LIMIT 2",
        parameters={"table": table},
    ).result_rows

    row_count = snapshots[0][0] if snapshots else 0
    prev_count = snapshots[1][0] if len(snapshots) > 1 else row_count
    delta_pct = 0.0 if prev_count == 0 else round(
        100.0 * (row_count - prev_count) / prev_count, 2
    )

    error_count_row = client.query(
        "SELECT count() FROM query_errors "
        "WHERE table_name = {table:String} "
        "AND occurred_at > now() - INTERVAL 1 HOUR",
        parameters={"table": table},
    ).result_rows
    error_count = error_count_row[0][0] if error_count_row else 0

    return TableHealth(
        table=table,
        replica_healthy=bool(is_healthy),
        replication_lag_seconds=int(lag_seconds),
        row_count=int(row_count),
        row_count_delta_pct=delta_pct,
        recent_query_errors=int(error_count),
        notes=str(notes),
    ).to_dict()


def mark_table_healthy(table: str) -> None:
    """Resets every simulated problem indicator for a table -- replica
    health, query errors, and row-count drift -- back to a clean state.
    Used by remediation. A full reset rather than only touching replica
    health, since remediation doesn't track which specific scenario was
    seeded; only resetting replica health left query-error and
    row-count-drift scenarios permanently unfixed, causing retries to
    fail on the same stale data indefinitely.
    """
    client = _get_client()
    client.command(
        "INSERT INTO replica_health (table_name, is_healthy, lag_seconds, notes, checked_at) "
        "VALUES ({table:String}, 1, 0, 'remediated by Airflow Reliability Copilot', now())",
        parameters={"table": table},
    )
    client.command(
        "DELETE FROM query_errors WHERE table_name = {table:String}",
        parameters={"table": table},
    )
    client.command(
        "INSERT INTO row_count_snapshots (table_name, snapshot_ts, row_count) "
        "VALUES ({table:String}, now(), {baseline:UInt64})",
        parameters={"table": table, "baseline": _BASELINE_ROW_COUNT},
    )


# --- Multi-scenario incident simulation ---
# Randomly picks healthy / replica-lag / query-errors / row-count-drift per
# run, so the AI reasons over varying evidence instead of one fixed case.

_SCENARIOS = ("replica_lag", "query_errors", "row_count_drift", "healthy")
_DEFAULT_WEIGHTS = (0.3, 0.25, 0.25, 0.2)  # ~20% of runs succeed outright
_BASELINE_ROW_COUNT = 5000


def _seed_replica_health(client, table: str, healthy: bool, lag_seconds: int, notes: str) -> None:
    client.command(
        "INSERT INTO replica_health (table_name, is_healthy, lag_seconds, notes, checked_at) "
        "VALUES ({table:String}, {healthy:UInt8}, {lag:UInt32}, {notes:String}, now())",
        parameters={"table": table, "healthy": int(healthy), "lag": lag_seconds, "notes": notes},
    )


def _seed_row_counts(client, table: str, prev_count: int, new_count: int) -> None:
    client.command(
        "INSERT INTO row_count_snapshots (table_name, snapshot_ts, row_count) "
        "VALUES ({table:String}, now() - toIntervalMinute(60), {prev:UInt64})",
        parameters={"table": table, "prev": prev_count},
    )
    client.command(
        "INSERT INTO row_count_snapshots (table_name, snapshot_ts, row_count) "
        "VALUES ({table:String}, now(), {new:UInt64})",
        parameters={"table": table, "new": new_count},
    )


def _seed_query_errors(client, table: str, count: int, message: str) -> None:
    for _ in range(count):
        client.command(
            "INSERT INTO query_errors (table_name, occurred_at, error_message) "
            "VALUES ({table:String}, now() - toIntervalMinute({m:UInt32}), {msg:String})",
            parameters={"table": table, "m": random.randint(0, 55), "msg": message},
        )


def seed_random_incident(
    table: str,
    weights: tuple[float, float, float, float] = _DEFAULT_WEIGHTS,
    force_scenario: str | None = None,
) -> str:
    """Inserts fresh ClickHouse readings for a scenario; returns its name.
    Pass force_scenario to skip the random pick (e.g. for a reliable
    on-camera demo run) -- falls back to random if it's not a valid name.
    """
    scenario = (
        force_scenario
        if force_scenario in _SCENARIOS
        else random.choices(_SCENARIOS, weights=list(weights), k=1)[0]
    )
    client = _get_client()

    if scenario == "replica_lag":
        _seed_replica_health(
            client, table, healthy=False, lag_seconds=random.randint(300, 1200),
            notes="replica-2 has not acknowledged writes -- suspected network partition",
        )
        _seed_row_counts(client, table, _BASELINE_ROW_COUNT, int(_BASELINE_ROW_COUNT * 1.01))
        _seed_query_errors(client, table, count=1, message="DB::Exception: Timeout waiting for replica ack")

    elif scenario == "query_errors":
        _seed_replica_health(client, table, healthy=True, lag_seconds=2, notes="healthy")
        _seed_row_counts(client, table, _BASELINE_ROW_COUNT, int(_BASELINE_ROW_COUNT * 1.01))
        _seed_query_errors(
            client, table, count=random.randint(8, 20),
            message="DB::Exception: Too many simultaneous queries -- write contention on 'orders'",
        )

    elif scenario == "row_count_drift":
        _seed_replica_health(client, table, healthy=True, lag_seconds=1, notes="healthy")
        dropped_pct = random.uniform(20, 45)
        new_count = int(_BASELINE_ROW_COUNT * (1 - dropped_pct / 100))
        _seed_row_counts(client, table, _BASELINE_ROW_COUNT, new_count)
        _seed_query_errors(client, table, count=0, message="")

    else:  # healthy
        _seed_replica_health(client, table, healthy=True, lag_seconds=1, notes="healthy")
        _seed_row_counts(client, table, _BASELINE_ROW_COUNT, int(_BASELINE_ROW_COUNT * 1.005))
        _seed_query_errors(client, table, count=0, message="")

    return scenario


# --- Incident audit log ---
# Every investigation writes one row here -- a real, queryable audit
# trail rather than just a claim in the README.

def log_incident(
    dag_run_id: str,
    source_dag_id: str,
    probable_cause: str,
    confidence: str,
    evidence: list[str],
    recommended_action: str,
    decision: str,
    decided_by: str,
) -> None:
    client = _get_client()
    client.command(
        "INSERT INTO incidents "
        "(dag_run_id, source_dag_id, probable_cause, confidence, evidence, "
        " recommended_action, decision, decided_by, created_at) "
        "VALUES ({dag_run_id:String}, {source_dag_id:String}, {probable_cause:String}, "
        " {confidence:String}, {evidence:String}, {recommended_action:String}, "
        " {decision:String}, {decided_by:String}, now())",
        parameters={
            "dag_run_id": dag_run_id,
            "source_dag_id": source_dag_id,
            "probable_cause": probable_cause,
            "confidence": confidence,
            "evidence": json.dumps(evidence),
            "recommended_action": recommended_action,
            "decision": decision,
            "decided_by": decided_by,
        },
    )
