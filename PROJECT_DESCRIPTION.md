# Project Description - Airflow Reliability Copilot

## Track entered
**Keep a Human in the Loop** (secondary fit: *Airflow Can Do That?!* wildcard, since the project also combines AI agents, ClickHouse, and Docker under Airflow orchestration).

## Problem statement
When a data pipeline fails, the response is almost always manual: an engineer gets paged, opens the Airflow UI, reads through task logs, separately queries the warehouse to check replication and data health, cross-references recent changes, and only then forms a hypothesis about what broke - before deciding whether it's safe to retry or remediate. This triage step is repetitive, slow, and doesn't scale with the number of pipelines a team runs. It also doesn't need a human for the *investigation* part - only for the *decision* part.

## Solution
Airflow Reliability Copilot automates the investigation and drafts the decision, but keeps a human firmly in control of any remediation action:

1. **Detect** - a pipeline (`orders_pipeline`) fails.
2. **Investigate, automatically** - a dedicated `reliability_investigation` DAG is triggered by the failure. It pulls the failing task's logs and metadata, then runs dynamically-mapped ClickHouse health checks (replication lag, row-count drift, recent query errors) across every table the pipeline touches.
3. **Diagnose with AI** - the collected evidence is passed to an LLM via Airflow's Common AI Provider (`LLMOperator`), using a Pydantic `output_type` schema (not free-text JSON parsing) for reliably structured output: probable cause, supporting evidence, confidence, and a recommended remediation action.
4. **Human approval, in Airflow itself** - instead of auto-remediating, the DAG pauses at a native `ApprovalOperator` (Human-in-the-Loop) task. The on-call engineer sees the AI's report directly in the Airflow UI's Required Actions tab - or via a real Google Chat notification - and approves or rejects the proposed fix.
5. **Remediate, only after approval** - on approval, Airflow triggers a `remediation_pipeline` DAG that resets the affected ClickHouse state and retries the original pipeline. On rejection, the incident is logged for manual follow-up and nothing automated runs. Every investigation, approved or rejected, is written to a ClickHouse audit table.

The result: minutes of manual log-spelunking become an auditable, versioned Airflow DAG run, and the human's job shrinks from "investigate from scratch" to "review a report and click approve or reject."

## Airflow features used
- **Human-in-the-Loop `ApprovalOperator`** (Airflow 3.1) - gates remediation on human sign-off, visible and actionable in the Airflow UI
- **Common AI Provider `LLMOperator`** - structured, provider-agnostic LLM calls (OpenAI/Anthropic/Gemini/Ollama) as a native Airflow operator, authenticated through an Airflow connection
- **Assets, both sides of the relationship** - `orders_pipeline` publishes an `orders_table` asset; `orders_freshness_monitor` is a DAG scheduled *entirely* off that asset event (`schedule=[Asset(...)]`, no cron), demonstrating genuine event-driven scheduling rather than a decorative Asset declaration
- **Dynamic task mapping** - per-table ClickHouse health checks expand at runtime based on which tables the failing pipeline touched
- **DAG-to-DAG triggering** - investigation → remediation → retry, chained via `TriggerDagRunOperator` / the Airflow REST API, driven by the HITL decision

## What makes this more than a single scripted demo
- **Multi-scenario simulation**: each `orders_pipeline` run randomly presents a healthy pass, replica lag, a query-error burst, or row-count drift, so the AI has to genuinely reason over varying evidence instead of pattern-matching one fixed failure.
- **Real audit trail**: every investigation -- approved or rejected -- is written to a ClickHouse `incidents` table, so "this is auditable" is something you can query, not just a claim.
- **CI**: `.github/workflows/ci.yml` installs Airflow and runs a real DAG-integrity test suite (import errors, expected DAG structure, asset-vs-cron scheduling) on every push.

## Technologies used
- Apache Airflow 3.1+ (scheduler, triggerer, webserver, standard provider)
- `apache-airflow-providers-common-ai`
- ClickHouse (telemetry / health data)
- Docker Compose
- Python 3.12
- Postgres (Airflow metadata database)

## Judging-criteria notes
- **Creativity/originality** - reframes AI-assisted incident response as a first-class, auditable Airflow DAG rather than a chat-based ops bot bolted onto Airflow.
- **Use of Airflow capabilities** - combines HITL, Common AI Provider, Assets, dynamic task mapping, and DAG-to-DAG triggering in a single coherent workflow.
- **Impact/usefulness** - directly targets a real, recurring cost center for data teams: incident triage time.
- **Technical implementation** - modular DAGs, typed helper modules, seeded reproducible ClickHouse demo data, tests for DAG integrity.
