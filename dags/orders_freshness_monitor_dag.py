"""
orders_freshness_monitor
=========================
Demonstrates genuine Airflow 3.1 event-driven scheduling: this DAG has no cron schedule at all, and runs whenever `orders_pipeline` publishes
a fresh `orders_table` asset event. Fires on every successful load, regardless of what `orders_pipeline` does downstream -- the
"freshness/consumer" side of asset-aware scheduling, as opposed to the failure-triggered side covered by `reliability_investigation`.

Airflow matches assets by URI, not Python object identity, so declaring `Asset("orders_table")` here independently of `orders_pipeline_dag.py`
still refers to the same asset.
"""
from __future__ import annotations

import logging

from airflow.sdk import Asset, dag, task

log = logging.getLogger(__name__)

ORDERS_TABLE_ASSET = Asset("orders_table")


@dag(
    dag_id="orders_freshness_monitor",
    schedule=[ORDERS_TABLE_ASSET],
    catchup=False,
    tags=["hackathon", "reliability-copilot", "assets", "event-driven"],
    doc_md=__doc__,
)
def orders_freshness_monitor():
    @task
    def record_freshness_event(**context) -> None:
        """A real deployment might update a freshness-SLA dashboard or
        kick off a dbt run here; kept simple to focus on the trigger.
        """
        triggering_events = context.get("triggering_asset_events", {})
        log.info(
            "orders_table asset updated -- freshness check triggered. "
            "Triggering asset events: %s",
            triggering_events,
        )

    record_freshness_event()


orders_freshness_monitor()

