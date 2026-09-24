"""
reliability_investigation
==========================
Triggered automatically when `orders_pipeline` fails. Gathers evidence,
asks an LLM for a structured root-cause report via the Common AI
Provider, then pauses on a native Human-in-the-Loop `ApprovalOperator`
before anything gets remediated.

Airflow 3.1 features shown here: dynamic task mapping
(`check_clickhouse_health`), the Common AI Provider (`LLMOperator`) with
structured Pydantic output, Human-in-the-Loop (`ApprovalOperator`), and
DAG-to-DAG triggering conditioned on the HITL response.
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import timedelta
from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task
from pydantic import BaseModel, Field

sys.path.append("/opt/airflow")
from include.clickhouse_client import check_table_health, log_incident  # noqa: E402
from include.notifiers import GoogleChatNotifier, LogOnCallNotifier  # noqa: E402

log = logging.getLogger(__name__)

RCA_SYSTEM_PROMPT = (
    "You are a data-reliability engineer's assistant embedded in an Apache "
    "Airflow pipeline. You'll be given a failed pipeline's task logs summary "
    "and ClickHouse health-check evidence as JSON. Diagnose the most likely "
    "root cause and recommend the least invasive fix that plausibly resolves it."
)


class RCAReport(BaseModel):
    """Structured root-cause report from root_cause_analysis. Defined at
    module scope so Airflow can register it for XCom deserialization.
    """

    probable_cause: str = Field(description="One or two sentences naming the most likely root cause")
    confidence: str = Field(description='One of "low", "medium", "high"')
    evidence: list[str] = Field(description="Specific facts from the input supporting the diagnosis")
    recommended_action: str = Field(
        description='One of "restart_replica_sync", "retry_pipeline_only", "escalate_to_oncall"'
    )
    recommended_action_rationale: str = Field(description="One sentence on why that action was chosen")


def _as_rca_report(value: RCAReport | dict) -> RCAReport:
    """Coerces an XCom'd rca_report back into a real RCAReport.

    Airflow's LLMOperator output_type is documented to push the Pydantic
    instance through XCom unchanged, but in practice downstream tasks can
    still receive a plain dict (observed even with the parameter
    type-hinted as RCAReport -- likely `from __future__ import
    annotations` turning that hint into a string rather than a live class
    reference, which XCom's auto-reconversion needs). Coercing explicitly
    here means it works regardless of which form actually arrives.
    """
    return value if isinstance(value, RCAReport) else RCAReport.model_validate(value)


on_call_notifier = LogOnCallNotifier(
    message=(
        "orders_pipeline failed and the AI investigation has produced a "
        "root-cause report. Review it and approve or reject remediation."
    )
)
google_chat_notifier = GoogleChatNotifier(
    message=(
        "orders_pipeline failed and the AI investigation has produced a "
        "root-cause report. Review it and approve or reject remediation."
    )
)


@dag(
    dag_id="reliability_investigation",
    schedule=None,  # triggered by orders_pipeline's failure callback
    catchup=False,
    tags=["hackathon", "reliability-copilot", "hitl", "ai"],
    # Lets a single "{{ ... }}" Jinja field render as a native dict instead
    # of a string -- needed so TriggerDagRunOperator's `conf` gets a real
    # dict from XCom.
    render_template_as_native_obj=True,
    doc_md=__doc__,
)
def reliability_investigation():
    @task
    def collect_task_logs(**context) -> dict:
        """Summarizes the failure from orders_pipeline's conf payload
        rather than re-parsing raw logs.
        """
        conf = context["dag_run"].conf or {}
        source_dag_id = conf.get("source_dag_id", "orders_pipeline")
        source_run_id = conf.get("source_run_id", "unknown")
        failed_task_ids = conf.get("failed_task_ids", [])

        summary = {
            "source_dag_id": source_dag_id,
            "source_run_id": source_run_id,
            "failed_task_ids": failed_task_ids,
            "summary": (
                f"Tasks {failed_task_ids} in DAG run {source_run_id} of "
                f"'{source_dag_id}' failed. See ClickHouse health evidence "
                f"for the likely cause."
            ),
        }
        log.info("Collected task log summary: %s", summary)
        return summary

    @task
    def get_tables_to_check(**context) -> list[str]:
        conf = context["dag_run"].conf or {}
        return conf.get("tables", ["orders"])

    @task
    def check_clickhouse_health(table: str) -> dict:
        return check_table_health(table)

    @task
    def build_evidence_bundle(log_summary: dict, health_results: list[dict]) -> str:
        """`health_results` is a mapped task's lazy XCom sequence --
        list(...) materializes it, or json.dumps silently collapses it to
        the string "LazyXComSequence()" instead of the real data.
        """
        bundle = {
            "task_log_summary": log_summary,
            "clickhouse_health": list(health_results),
        }
        return json.dumps(bundle, indent=2, default=str)

    @task
    def build_approval_body(rca_report: RCAReport) -> str:
        rca_report = _as_rca_report(rca_report)
        evidence_lines = "\n".join(f"  - {e}" for e in rca_report.evidence)
        return (
            f"**Probable cause:** {rca_report.probable_cause}\n"
            f"**Confidence:** {rca_report.confidence}\n\n"
            f"**Evidence:**\n{evidence_lines}\n\n"
            f"**Recommended action:** {rca_report.recommended_action}\n"
            f"**Why:** {rca_report.recommended_action_rationale}\n\n"
            f"Approve to run `remediation_pipeline` with this action, or "
            f"reject to leave the incident for manual investigation."
        )

    @task
    def trigger_remediation_conf(rca_report: RCAReport, **context) -> dict:
        rca_report = _as_rca_report(rca_report)
        conf = context["dag_run"].conf or {}
        return {
            "source_dag_id": conf.get("source_dag_id", "orders_pipeline"),
            "recommended_action": rca_report.recommended_action,
            "probable_cause": rca_report.probable_cause,
            "tables": conf.get("tables", ["orders"]),
        }

    @task
    def log_manual_followup(rca_report: RCAReport) -> None:
        rca_report = _as_rca_report(rca_report)
        log.warning(
            "Remediation was rejected by the on-call engineer. Logging "
            "incident for manual follow-up. AI report was:\n%s",
            rca_report,
        )

    @task
    def record_incident(rca_report: RCAReport, hitl_output: dict, **context) -> None:
        """Writes one row to ClickHouse's `incidents` audit table,
        regardless of the human's decision -- deliberately independent of
        the approve/reject branch so it always runs.
        """
        rca_report = _as_rca_report(rca_report)
        conf = context["dag_run"].conf or {}
        chosen = hitl_output.get("chosen_options", [])
        decision = "approved" if "Approve" in chosen else "rejected"
        responded_by_user = hitl_output.get("responded_by_user") or {}
        if isinstance(responded_by_user, dict):
            decided_by = (
                responded_by_user.get("name")
                or responded_by_user.get("id")
                or "unknown"
            )
        else:
            decided_by = str(responded_by_user) if responded_by_user else "unknown"

        log_incident(
            dag_run_id=context["dag_run"].run_id,
            source_dag_id=conf.get("source_dag_id", "orders_pipeline"),
            probable_cause=rca_report.probable_cause,
            confidence=rca_report.confidence,
            evidence=rca_report.evidence,
            recommended_action=rca_report.recommended_action,
            decision=decision,
            decided_by=decided_by,
        )
        log.info("Recorded incident (decision=%s) to ClickHouse audit log", decision)

    @task.branch
    def branch_on_decision(hitl_output: dict) -> str:
        chosen = hitl_output.get("chosen_options", [])
        return "trigger_remediation_conf" if "Approve" in chosen else "log_manual_followup"

    log_summary = collect_task_logs()
    tables = get_tables_to_check()
    health_results = check_clickhouse_health.expand(table=tables)
    evidence = build_evidence_bundle(log_summary, health_results)

    rca = LLMOperator(
        task_id="root_cause_analysis",
        llm_conn_id="pydanticai_default",
        system_prompt=RCA_SYSTEM_PROMPT,
        prompt="Investigate this pipeline failure:\n\n{{ ti.xcom_pull(task_ids='build_evidence_bundle') }}",
        output_type=RCAReport,
        retries=3,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
    )
    evidence >> rca

    approval_body = build_approval_body(rca.output)

    approval_gate = ApprovalOperator(
        task_id="approval_gate",
        subject="Approve automated remediation for orders_pipeline?",
        body="{{ ti.xcom_pull(task_ids='build_approval_body') }}",
        defaults="Reject",
        response_timeout=timedelta(hours=4),
        notifiers=[on_call_notifier, google_chat_notifier],
    )
    approval_body >> approval_gate

    decision = branch_on_decision(approval_gate.output)
    incident_record = record_incident(rca.output, approval_gate.output)
    remediation_conf = trigger_remediation_conf(rca.output)
    trigger_remediation = TriggerDagRunOperator(
        task_id="trigger_remediation",
        trigger_dag_id="remediation_pipeline",
        trigger_run_id="remediate__{{ ts_nodash }}",
        conf="{{ ti.xcom_pull(task_ids='trigger_remediation_conf') }}",
    )
    followup = log_manual_followup(rca.output)

    decision >> [remediation_conf, followup]
    remediation_conf >> trigger_remediation


reliability_investigation()
