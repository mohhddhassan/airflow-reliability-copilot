"""
Notifiers used alongside the Human-in-the-Loop operators. HITL operators
accept a list of notifiers, so LogOnCallNotifier (always-on log line) and
GoogleChatNotifier (real push notification) run side by side.
"""
from __future__ import annotations

import logging
import os

import requests
from airflow.sdk.bases.notifier import BaseNotifier

log = logging.getLogger("airflow.reliability_copilot.notifiers")


class LogOnCallNotifier(BaseNotifier):
    """Logs loudly when a HITL action needs a response."""

    template_fields = ("message",)

    def __init__(self, message: str) -> None:
        self.message = message

    def notify(self, context) -> None:
        dag_id = context["dag"].dag_id
        run_id = context["dag_run"].run_id
        log.warning(
            "\n"
            "============================================================\n"
            " HUMAN APPROVAL NEEDED - Airflow Reliability Copilot\n"
            " DAG: %s | Run: %s\n"
            " %s\n"
            " Open the Airflow UI -> this task instance -> Required Actions\n"
            "============================================================",
            dag_id,
            run_id,
            self.message,
        )


class GoogleChatNotifier(BaseNotifier):
    """Posts a real notification to a Google Chat space via an incoming
    webhook. Set GOOGLE_CHAT_WEBHOOK_URL (see .env.example) to enable --
    silently does nothing if it's not configured, so this notifier is
    always safe to include even before you've set one up.

    Get a webhook URL: open a Google Chat space -> space name -> Apps &
    integrations -> Webhooks -> Add webhook -> copy the URL.
    """

    template_fields = ("message", "airflow_ui_url")

    def __init__(self, message: str, airflow_ui_url: str = "http://localhost:8080") -> None:
        self.message = message
        self.airflow_ui_url = airflow_ui_url

    def notify(self, context) -> None:
        webhook_url = os.environ.get("GOOGLE_CHAT_WEBHOOK_URL")
        if not webhook_url:
            log.debug("GOOGLE_CHAT_WEBHOOK_URL not set -- skipping Google Chat notification")
            return

        dag_id = context["dag"].dag_id
        run_id = context["dag_run"].run_id
        text = (
            f"*Human approval needed -- Airflow Reliability Copilot*\n"
            f"{self.message}\n"
            f"DAG: `{dag_id}` | Run: `{run_id}`\n"
            f"{self.airflow_ui_url}"
        )
        try:
            resp = requests.post(webhook_url, json={"text": text}, timeout=10)
            resp.raise_for_status()
        except requests.RequestException:
            # Never let a notification failure block the pipeline.
            log.exception("Failed to post Google Chat notification")

