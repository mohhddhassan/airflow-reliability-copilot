"""
orders_pipeline
================
A deliberately fragile ETL DAG for demoing Airflow Reliability Copilot. Each run simulates a random condition (healthy / replica lag / query
errors / row-count drift) and checks ClickHouse health before "publishing".

Gotchas worth knowing if you're building something similar:
1. The DAG-level `on_failure_callback=` kwarg silently never fires in
   Airflow 3.1.x (apache/airflow#63374) -- set it in `default_args` instead.
2. Task-level callbacks run in the Task SDK's isolated worker, which has
   no direct DB access, so triggering another DAG has to go through the
   REST API rather than `airflow.api.common.trigger_dag()`.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone

import requests
from airflow.sdk import Asset, dag, task

sys.path.append("/opt/airflow")
from include.clickhouse_client import check_table_health, seed_random_incident  # noqa: E402

log = logging.getLogger(__name__)

ORDERS_TABLE_ASSET = Asset("orders_table")
PIPELINE_TABLES = ["orders"]

QUERY_ERROR_THRESHOLD = 5
ROW_COUNT_DELTA_THRESHOLD_PCT = 15.0

AIRFLOW_API_BASE_URL = "http://airflow-webserver:8080"

BOT_USERNAME = "reliability-bot"


def _get_api_token() -> str:
    """Gets a JWT for the Airflow API. Works under either auth mode this
    project supports (see docker-compose.recording.yml): falls back to a
    placeholder login when auth is bypassed, or reads the real generated
    password for the bot account when named users are configured.
    """
    username, password = "admin", "admin"

    passwords_file = os.environ.get(
        "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE",
        "/opt/airflow/logs/simple_auth_manager_passwords.json.generated",
    )
    try:
        with open(passwords_file) as f:
            passwords = json.load(f)
        if BOT_USERNAME in passwords:
            username, password = BOT_USERNAME, passwords[BOT_USERNAME]
    except (OSError, json.JSONDecodeError):
        pass

    resp = requests.post(
        f"{AIRFLOW_API_BASE_URL}/auth/token",
        json={"username": username, "password": password},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def trigger_investigation_on_failure(context) -> None:
    """Task-level on_failure_callback (see default_args below). Triggers
    reliability_investigation via the REST API whenever a task fails.
    """
    ti = context["ti"]
    run_id = context["run_id"]
    ts_nodash = context["ts_nodash"]

    log.warning(
        "orders_pipeline task %s failed (run %s) - triggering reliability_investigation",
        ti.task_id,
        run_id,
    )

    try:
        token = _get_api_token()
        resp = requests.post(
            f"{AIRFLOW_API_BASE_URL}/api/v2/dags/reliability_investigation/dagRuns",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "dag_run_id": f"investigate__{ts_nodash}",
                "logical_date": datetime.now(timezone.utc).isoformat(),
                "conf": {
                    "source_dag_id": "orders_pipeline",
                    "source_run_id": run_id,
                    "failed_task_ids": [ti.task_id],
                    "tables": PIPELINE_TABLES,
                },
            },
            timeout=10,
        )
        resp.raise_for_status()
        log.info("Triggered reliability_investigation: %s", resp.json())
    except (requests.RequestException, OSError, json.JSONDecodeError, RuntimeError):
        # Don't let a failed trigger mask the original task failure.
        log.exception("Failed to trigger reliability_investigation")


@dag(
    dag_id="orders_pipeline",
    schedule=None,  # manual/system-triggered only -- no cron for this demo
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["hackathon", "orders", "reliability-copilot"],
    default_args={"on_failure_callback": trigger_investigation_on_failure},
    doc_md=__doc__,
)
def orders_pipeline():
    @task
    def extract_orders() -> list[dict]:
        return [{"order_id": i, "customer_id": i % 500} for i in range(1, 51)]

    @task(outlets=[ORDERS_TABLE_ASSET])
    def load_orders(batch: list[dict]) -> int:
        log.info("Loaded %s orders into ClickHouse `orders` table", len(batch))
        return len(batch)

    @task
    def simulate_incident(**context) -> str:
        """Randomly picks this run's scenario so the AI sees genuine
        variety instead of one canned failure every time -- except on a
        post-remediation retry, where re-rolling would overwrite whatever
        fix was just applied and make it look like remediation never
        works. Retries triggered by remediation_pipeline just confirm
        the current (hopefully now-healthy) state instead.
        """
        conf = context["dag_run"].conf or {}
        if conf.get("triggered_by") == "remediation_pipeline":
            log.info("Post-remediation retry -- skipping re-simulation, checking current health as-is")
            return "post_remediation_retry"

        force_scenario = conf.get("force_scenario")
        scenario = seed_random_incident("orders", force_scenario=force_scenario)
        log.info("Simulated incident scenario for this run: %s", scenario)
        return scenario

    @task
    def check_replica_before_publish(loaded_count: int, scenario: str) -> None:
        """Fails the run if the replica is unhealthy, errors spiked, or
        row counts drifted -- whichever `simulate_incident` chose.
        """
        health = check_table_health("orders")
        log.info("orders table health (simulated scenario=%s): %s", scenario, health)

        problems = []
        if not health["replica_healthy"]:
            problems.append(
                f"replica unhealthy (lag={health['replication_lag_seconds']}s): {health['notes']}"
            )
        if health["recent_query_errors"] > QUERY_ERROR_THRESHOLD:
            problems.append(
                f"{health['recent_query_errors']} query errors in the last hour: {health['notes']}"
            )
        if abs(health["row_count_delta_pct"]) > ROW_COUNT_DELTA_THRESHOLD_PCT:
            problems.append(
                f"row count changed {health['row_count_delta_pct']}% since last snapshot"
            )

        if problems:
            raise RuntimeError(
                "Refusing to publish 'orders': " + "; ".join(problems) +
                f". Loaded {loaded_count} rows but did not mark them published."
            )

    batch = extract_orders()
    loaded = load_orders(batch)
    scenario = simulate_incident()
    check_replica_before_publish(loaded, scenario)


orders_pipeline()
