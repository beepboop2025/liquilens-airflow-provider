"""Execute the committed DAG and its real operator task against Airflow's DB."""

from __future__ import annotations

from datetime import UTC, datetime

from airflow.sdk import DagRunState

from tests.dags.liquilens_evidence_smoke import dag

run = dag.test(
    logical_date=datetime(2026, 8, 24, 12, tzinfo=UTC),
    run_after=datetime(2026, 8, 24, 12, tzinfo=UTC),
)
if run.state != DagRunState.SUCCESS:
    raise SystemExit(f"DAG finished in unexpected state: {run.state}")
print(f"dag_id={run.dag_id} run_id={run.run_id} state={run.state}")
