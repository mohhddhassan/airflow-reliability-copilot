# Airflow Reliability Copilot

**Beyond the DAG: Data Engineering Hackathon 2026 - Astronomer**
**Track:** Keep a Human in the Loop (Human-in-the-Loop) - also qualifies for the *Airflow Can Do That?!* wildcard track
**License:** MIT (see [LICENSE](./LICENSE))

## What it does

When a production pipeline fails, someone has to stop what they're doing, open the Airflow UI, read task logs, log into ClickHouse to check replica health, correlate row counts and replication lag, and eventually guess at a root cause before doing anything about it. That whole loop - detect, investigate, diagnose, decide, act - usually happens by hand, under time pressure.

**Airflow Reliability Copilot turns that loop into a DAG.**

1. A production pipeline (`orders_pipeline`) runs. Each run randomly simulates one of four real-world conditions - a healthy pass, replica lag / a suspected network partition, a burst of query errors, or unexplained row-count drift - so the demo shows genuine variety instead of one scripted failure every time.
2. On failure, Airflow automatically triggers an **investigation DAG** (`reliability_investigation`) - no human has to notice the failure first.
3. The investigation DAG collects the failing task's logs and run metadata, then queries **ClickHouse** for replication status, table health, row-count drift, and recent query errors.
4. That evidence is handed to an LLM through Airflow's **Common AI Provider** (`LLMOperator`), which returns a structured root-cause report: probable cause, confidence, evidence used, and a recommended remediation action.
5. Airflow **pauses the workflow** with a native **Human-in-the-Loop `ApprovalOperator`** and shows the AI's report to an on-call engineer, who approves or rejects the proposed fix directly in the Airflow UI (or via the REST API).
6. **Every investigation is written to a ClickHouse audit-log table** (`incidents`) - probable cause, confidence, evidence, recommended action, and the human's decision - regardless of whether it was approved or rejected. A real, queryable audit trail, not just a claim in this README.
7. On approval, Airflow **triggers a remediation DAG** that resets whatever ClickHouse state the diagnosis pointed at (replica health, query-error backlog, or row-count drift) and re-runs the original pipeline to confirm the fix actually worked. On rejection, the incident is logged as "needs manual investigation" and no automated action is taken.

Separately, a fourth DAG (`orders_freshness_monitor`) demonstrates genuine **event-driven scheduling**: it has no cron schedule at all and runs entirely off an Asset event published whenever `orders_pipeline` loads fresh data.

Airflow is the orchestration engine throughout - not a hidden AI agent with Airflow bolted on the side. Every step (detection, evidence-gathering, AI reasoning, human approval, remediation, audit logging) is a first-class Airflow task, so it's versioned, retried, logged, and auditable the same way any other pipeline is.

## Why this fits the hackathon

| Requirement | How this project meets it |
|---|---|
| Apache Airflow 3.1+ | Uses Airflow 3.1 HITL operators (`ApprovalOperator`), Assets (both producer and consumer sides), and TaskFlow |
| Airflow features showcased | Human-in-the-Loop, Common AI Provider (`LLMOperator`), genuine event-driven scheduling (a DAG scheduled purely off an Asset, no cron), dynamic task mapping (per-table ClickHouse health checks), DAG-to-DAG triggering |
| Track: Keep a Human in the Loop | The remediation step is gated on a real `ApprovalOperator` task - nothing destructive runs without a human clicking Approve - and every decision is permanently logged |
| Open source | MIT licensed, single public repo, no proprietary dependencies |
| Created for the hackathon | Built from scratch for Beyond the DAG 2026 |

## Architecture

See [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md) for the full diagram and data flow. Short version:

```
orders_pipeline (DAG)
   ├─ each run: simulate_incident (random scenario -- healthy / replica lag /
   │    query errors / row-count drift)
   └─ fails ──on_failure_callback──▶ reliability_investigation (DAG)
                                        ├─ collect_task_logs
                                        ├─ check_clickhouse_health  (dynamically mapped, one per table)
                                        ├─ root_cause_analysis      (LLMOperator, Common AI Provider)
                                        ├─ approval_gate            (ApprovalOperator - HITL)
                                        ├─ record_incident          (writes to ClickHouse `incidents` audit log)
                                        └─ branch:
                                             ├─ approved ▶ trigger remediation_pipeline (DAG)
                                             └─ rejected ▶ log_manual_followup
remediation_pipeline (DAG)
   ├─ apply_remediation_action
   └─ retry_orders_pipeline

orders_freshness_monitor (DAG, no cron schedule)
   └─ triggered purely by the orders_table Asset event orders_pipeline publishes
```

## Tech stack

- **Orchestrator:** Apache Airflow 3.1+ (`apache/airflow:3.1.5`)
- **Database:** ClickHouse (health/replication/row-count telemetry)
- **AI:** `apache-airflow-providers-common-ai` (`LLMOperator`), configurable to any of OpenAI / Anthropic / Gemini / Ollama
- **HITL:** `apache-airflow-providers-standard` `ApprovalOperator`
- **Containers:** Docker Compose
- **Metadata DB:** Postgres (standard Airflow metadata store)

## Project layout

```
airflow-reliability-copilot/
├── dags/
│   ├── orders_pipeline_dag.py            # the pipeline; simulates a random scenario each run
│   ├── reliability_investigation_dag.py  # detect → investigate → AI → HITL → audit log
│   ├── remediation_pipeline_dag.py       # applies the approved fix
│   └── orders_freshness_monitor_dag.py   # scheduled purely off an Asset event (no cron)
├── include/
│   ├── clickhouse_client.py             # ClickHouse helper: health checks, scenario seeding, audit log
│   ├── notifiers.py                     # HITL notifier used to "page" the on-call engineer
│   └── clickhouse_init/
│       └── 001_init.sql                 # seed schema (incl. incidents audit table) + sample data
├── docs/
│   └── ARCHITECTURE.md
├── tests/
│   └── test_dag_integrity.py            # DAG import / structure sanity checks, run in CI
├── .github/workflows/
│   └── ci.yml                           # GitHub Actions: installs Airflow + runs the test suite
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
├── PROJECT_DESCRIPTION.md
└── LICENSE
```

## Running it locally

### Prerequisites

- Docker + Docker Compose
- An API key for at least one LLM provider (OpenAI, Anthropic, Gemini, or a local Ollama endpoint)

### 1. Configure environment

```bash
cp .env.example .env
# edit .env and set your LLM provider key, e.g.:
#   GOOGLE_API_KEY=...
```

### 2. Start everything

```bash
docker compose up --build
```

This starts:
- `postgres` - Airflow metadata database
- `clickhouse` - seeded with a baseline `orders` schema; each `orders_pipeline` run then simulates its own random condition on top of that baseline (see "Multi-scenario failure simulation" below)
- `airflow-init` - one-shot DB migration
- `airflow-webserver`, `airflow-scheduler`, `airflow-triggerer`, `airflow-dag-processor`

### 3. Open the Airflow UI

Go to **http://localhost:8080**. No login is required by default - this project runs with `SIMPLE_AUTH_MANAGER_ALL_ADMINS=True`, a Simple Auth Manager option meant for local/dev use, so anyone opening the UI is automatically admin. Deliberately zero-friction so anyone cloning this repo can go straight from `docker compose up` to the demo.

**Want to see real user identity instead** - HITL approvals and the ClickHouse `incidents` audit log showing an actual username instead of "Anonymous"? Use the recording override instead of editing `docker-compose.yml` directly:

```bash
docker compose down -v
docker compose -f docker-compose.yml -f docker-compose.recording.yml up --build
docker compose exec airflow-webserver cat /opt/airflow/logs/simple_auth_manager_passwords.json.generated
```

That logs you in as a real named user (`hussain` by default - rename it in `docker-compose.recording.yml`), and gives the automated failure-callback its own separate `reliability-bot` service account, so the audit trail can distinguish "a human approved this" from "the system triggered an investigation" - mirroring how a real deployment separates human and service identities. See `docker-compose.recording.yml` for details. To go back to the default, just drop the extra `-f` flag (after another `down -v`, since the auth mode is set at container start).

Simple Auth Manager itself is explicitly documented as **development/testing only**. A real deployment would use Airflow's FAB auth manager (traditional per-user RBAC) or an enterprise auth manager tied to your company's actual SSO/identity provider - either way, every engineer has their own individual account, never one shared admin login.

### 4. Configure the Common AI Provider connection

The Common AI Provider is built on PydanticAI. `docker-compose.yml` already sets it via `AIRFLOW_CONN_PYDANTICAI_DEFAULT`, configured for Gemini:

```
{"conn_type": "pydanticai", "password": "<your key>", "extra": {"model": "google:gemini-3.6-flash"}}
```

Set `GOOGLE_API_KEY` in `.env` and you're done - swap `requirements.txt`'s provider extra (`[google]` → `[anthropic]` or `[openai]`) and the `extra.model` value in `docker-compose.yml` if you'd rather use a different provider or a local Ollama endpoint.

### 5. Run the demo

1. Unpause `orders_pipeline`, `reliability_investigation`, `remediation_pipeline`, and `orders_freshness_monitor` in the UI (or `docker compose exec airflow-scheduler airflow dags unpause <dag_id>` for each).
2. Trigger `orders_pipeline` a few times. Each run randomly simulates a healthy pass, replica lag, a query-error burst, or row-count drift -- about 80% of runs fail one of those checks and kick off the investigation; the rest succeed outright (and separately trigger `orders_freshness_monitor` via the asset event).
3. Watch `reliability_investigation` fire automatically on a failing run. Open the DAG run and watch `root_cause_analysis` produce a structured report.
4. Go to the `approval_gate` task instance's **Required Actions** tab, review the AI's report, and click **Approve** (or **Reject**).
5. Watch `remediation_pipeline` run and `orders_pipeline` retry successfully. Query ClickHouse's `incidents` table (`SELECT * FROM reliability_demo.incidents ORDER BY created_at DESC`) to see the full audit trail, including rejected investigations.

### 6. Tear down

```bash
docker compose down -v
```

## Demo video

A ≤3 minute walkthrough of the failure → investigation → AI diagnosis → human approval → remediation loop is linked here: `<ADD_DEMO_VIDEO_LINK_HERE>`.

## Airflow 3.1 features used

- **Human-in-the-Loop (`ApprovalOperator`)** - gates remediation on human approval
- **Common AI Provider (`LLMOperator`)** - structured root-cause analysis from an LLM, using an Airflow-managed connection and a Pydantic `output_type` schema (not free-text JSON parsing) for reliably structured output
- **Assets, both producer and consumer sides** - `orders_pipeline` publishes an `orders_table` asset on every successful load; `orders_freshness_monitor` is scheduled *purely* off that asset event (`schedule=[ORDERS_TABLE_ASSET]`, no cron at all) -- genuine event-driven scheduling, not a decorative asset declaration
- **Dynamic task mapping** - `check_clickhouse_health` expands into one mapped task per table being investigated
- **DAG-to-DAG triggering** - investigation triggers remediation only after approval, via `TriggerDagRunOperator`. `orders_pipeline`'s own failure hook uses a task-level `default_args` callback that calls the Airflow REST API directly (not `trigger_dag()`), working around a known Airflow 3.1 bug where the DAG-level `on_failure_callback=` kwarg silently never fires (apache/airflow#63374) and around the Task SDK's restriction on direct metadata-DB access from task code

## Multi-scenario failure simulation

Each `orders_pipeline` run randomly presents one of four conditions (see `include/clickhouse_client.py`'s `seed_random_incident`):

| Scenario | Weight | What it simulates |
|---|---|---|
| Healthy | 20% | Nothing wrong -- pipeline just succeeds |
| Replica lag | 30% | Unhealthy replica, suspected network partition |
| Query error burst | 25% | 8-20 recent write-contention errors, replica otherwise fine |
| Row-count drift | 25% | 20-45% row-count drop since the last snapshot (simulated data loss) |

This means the AI has to actually reason over whatever evidence it's given each run rather than pattern-matching one fixed scenario -- and the demo shows real variety instead of the same canned failure every time.

## Incident audit log

Every investigation -- approved or rejected -- writes one row to ClickHouse's `incidents` table via `record_incident` / `log_incident`: timestamp, probable cause, confidence, evidence, recommended action, and the human's decision. Query it directly:

```sql
SELECT created_at, decision, confidence, probable_cause, recommended_action
FROM reliability_demo.incidents
ORDER BY created_at DESC;
```

## Notifications

By default, a pending approval only shows up if you're watching the Airflow UI. Set `GOOGLE_CHAT_WEBHOOK_URL` in `.env` for a real push notification instead: open a Google Chat space → space name → Apps & integrations → Webhooks → Add webhook → copy the URL. Leave it unset and `GoogleChatNotifier` just no-ops silently - `LogOnCallNotifier` (a loud scheduler log line) always fires regardless.

## Continuous integration

`.github/workflows/ci.yml` installs Airflow 3.1.5 and this project's providers, then runs `tests/test_dag_integrity.py` -- DAG import errors, expected DAG IDs, the failure-callback wiring, and (for `orders_freshness_monitor`) that it's genuinely asset-scheduled rather than cron-scheduled. Note: Airflow's own published constraints file for 3.1.5 pins `apache-airflow-providers-common-compat` to a version older than what `apache-airflow-providers-common-ai` requires, so the workflow installs the extra providers unconstrained (letting pip resolve compatible newer versions) after Airflow core itself is pinned via the constraints file.

## License

MIT - see [LICENSE](./LICENSE). You retain ownership of your fork/contributions; the repository is public per hackathon rules.
