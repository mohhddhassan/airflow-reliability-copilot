"""
Basic DAG integrity checks - the kind of tests judges (and CI) can run
without spinning up the full Docker Compose stack, since they only need
Airflow + the providers installed.

Run with:
    pytest tests/test_dag_integrity.py
"""
from __future__ import annotations

import os
import sys

import pytest

DAGS_DIR = os.path.join(os.path.dirname(__file__), "..", "dags")
PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")

sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, DAGS_DIR)

EXPECTED_DAG_IDS = {
    "orders_pipeline",
    "reliability_investigation",
    "remediation_pipeline",
    "orders_freshness_monitor",
}


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    return DagBag(dag_folder=DAGS_DIR, include_examples=False)


def test_no_import_errors(dagbag):
    assert not dagbag.import_errors, (
        f"DAG import errors found: {dagbag.import_errors}"
    )


def test_expected_dags_present(dagbag):
    found = set(dagbag.dag_ids)
    missing = EXPECTED_DAG_IDS - found
    assert not missing, f"Expected DAGs missing from DagBag: {missing}"


def test_orders_pipeline_has_failure_callback(dagbag):
    d = dagbag.dags["orders_pipeline"]
    # Set via default_args, not the DAG-level kwarg (apache/airflow#63374).
    assert d.default_args.get("on_failure_callback") is not None, (
        "orders_pipeline must trigger reliability_investigation on failure"
    )


def test_investigation_dag_has_no_schedule(dagbag):
    d = dagbag.dags["reliability_investigation"]
    assert d.timetable.summary == "None"


def test_remediation_dag_has_no_schedule(dagbag):
    d = dagbag.dags["remediation_pipeline"]
    assert d.timetable.summary == "None"


def test_freshness_monitor_is_asset_scheduled(dagbag):
    d = dagbag.dags["orders_freshness_monitor"]
    assert "Asset" in d.timetable.summary or "asset" in d.timetable.summary.lower()


def test_every_dag_has_tags(dagbag):
    for dag_id, d in dagbag.dags.items():
        assert d.tags, f"DAG {dag_id} should declare tags"
