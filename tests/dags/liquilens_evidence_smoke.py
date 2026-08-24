"""A real Airflow DAG used by hosted consumer smoke tests."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from airflow.sdk import DAG

from liquilens_airflow_provider import VerifyEvidenceCarrierOperator

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "carrier.json"

with DAG(
    dag_id="liquilens_evidence_smoke",
    description="Offline verification of one rights-bounded evidence carrier",
    schedule=None,
    start_date=datetime(2026, 8, 24, tzinfo=UTC),
    catchup=False,
    tags=["liquilens", "evidence", "offline"],
) as dag:
    verify_evidence = VerifyEvidenceCarrierOperator(
        task_id="verify_evidence",
        carrier_path=str(FIXTURE),
        evaluated_at=datetime(2026, 8, 24, 12, tzinfo=UTC),
    )
