"""
remediation_pipeline
=====================
Triggered by `reliability_investigation` after a human approves the AI's
recommended action. Applies the fix, then re-triggers `orders_pipeline`.

Uses `TriggerDagRunOperator` rather than `airflow.api.common.trigger_dag`
since task code has no direct Airflow-metadata-DB access in Airflow 3.
"""
from __future__ import annotations

import logging
import sys

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task

sys.path.append("/opt/airflow")
from include.clickhouse_client import mark_table_healthy  # noqa: E402

log = logging.getLogger(__name__)

# The AI *recommends*; this dict is what actually decides what code runs.
REMEDIATION_ACTIONS = {
    "restart_replica_sync": "restart_replica_sync",
    "retry_pipeline_only": "retry_pipeline_only",
    "escalate_to_oncall": "escalate_to_oncall",
}

# Hardcoded, not read from conf -- TriggerDagRunOperator needs its target
# dag_id at parse time.
SOURCE_PIPELINE_DAG_ID = "orders_pipeline"


@dag(
    dag_id="remediation_pipeline",
    schedule=None,  # triggered by reliability_investigation after HITL approval
    catchup=False,
    tags=["hackathon", "reliability-copilot", "remediation"],
    doc_md=__doc__,
)
def remediation_pipeline():
    @task
    def apply_remediation_action(**context) -> str:
        conf = context["dag_run"].conf or {}
        action = conf.get("recommended_action", "escalate_to_oncall")
        tables = conf.get("tables", ["orders"])
        cause = conf.get("probable_cause", "unknown")

        resolved_action = REMEDIATION_ACTIONS.get(action, "escalate_to_oncall")
        log.info(
            "Applying remediation action=%s for tables=%s (probable_cause=%r)",
            resolved_action,
            tables,
            cause,
        )

        # Both "fix it" actions do a full health reset, not just replica
        # health -- remediation doesn't know which specific scenario was
        # simulated, so only resetting replica health left query-error and
        # row-count-drift cases permanently unfixed, causing every retry
        # to fail on the same stale data no matter how many times it's
        # approved.
        if resolved_action in ("restart_replica_sync", "retry_pipeline_only"):
            for table in tables:
                mark_table_healthy(table)
            log.info("Reset ClickHouse health state for tables: %s", tables)
        else:
            log.warning(
                "Action requires a human; not something this DAG can automate. "
                "Escalating to on-call instead of auto-remediating."
            )

        return resolved_action

    @task
    def log_escalation(resolved_action: str) -> None:
        log.warning(
            "Remediation action %r requires manual follow-up; skipping "
            "automatic retry of %s.",
            resolved_action,
            SOURCE_PIPELINE_DAG_ID,
        )

    @task.branch
    def branch_on_action(resolved_action: str) -> str:
        if resolved_action == "escalate_to_oncall":
            return "log_escalation"
        return "retry_orders_pipeline"

    resolved = apply_remediation_action()
    decision = branch_on_action(resolved)

    # ts_nodash gives a short, bounded run id -- chaining onto
    # dag_run.run_id instead compounds every retry cycle onto the
    # previous run's already-long id, eventually exceeding the database's
    # run_id column length and crashing with a 500 error.
    retry = TriggerDagRunOperator(
        task_id="retry_orders_pipeline",
        trigger_dag_id=SOURCE_PIPELINE_DAG_ID,
        trigger_run_id="retry_after_remediation__{{ ts_nodash }}",
        conf={"triggered_by": "remediation_pipeline"},
    )
    escalate = log_escalation(resolved)

    decision >> [retry, escalate]


remediation_pipeline()
