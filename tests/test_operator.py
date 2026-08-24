from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from airflow.exceptions import AirflowException
from airflow.sdk import DAG
from liquilens_evidence import issue_evidence_carrier

from liquilens_airflow_provider import VerifyEvidenceCarrierOperator

FIXTURES = Path(__file__).parent / "fixtures"
EVALUATED_AT = datetime(2026, 8, 24, 12, tzinfo=UTC)


def _operator(path: Path) -> VerifyEvidenceCarrierOperator:
    with DAG(
        dag_id=f"test_{path.stem}",
        schedule=None,
        start_date=datetime(2026, 8, 24, tzinfo=UTC),
    ):
        return VerifyEvidenceCarrierOperator(
            task_id="verify",
            carrier_path=str(path),
            evaluated_at=EVALUATED_AT,
        )


def test_valid_carrier_returns_full_policy_bounded_view() -> None:
    result = _operator(FIXTURES / "carrier.json").execute({})  # type: ignore[arg-type]

    assert result["disposition"] == "full"
    assert result["reason_codes"] == []
    assert result["record_hash"] == result["evidence"]["record_hash"]
    assert result["evidence"]["authority"] == {
        "financial_authority": "none",
        "can_execute": False,
        "can_recommend": False,
        "is_credit_rating": False,
    }


def test_tampered_carrier_fails_closed(tmp_path: Path) -> None:
    carrier = json.loads((FIXTURES / "carrier.json").read_text())
    carrier["payload"]["purpose"] = "tampered"
    path = tmp_path / "tampered.json"
    path.write_text(json.dumps(carrier))

    with pytest.raises(AirflowException, match="verification failed"):
        _operator(path).execute({})  # type: ignore[arg-type]


def test_duplicate_keys_and_oversized_files_fail_closed(tmp_path: Path) -> None:
    duplicate = tmp_path / "duplicate.json"
    duplicate.write_text('{"schema_version":"1.0","schema_version":"1.0"}')
    with pytest.raises(AirflowException, match="strict UTF-8 JSON"):
        _operator(duplicate).execute({})  # type: ignore[arg-type]

    oversized = tmp_path / "oversized.json"
    oversized.write_bytes(b" " * 1_048_577)
    with pytest.raises(AirflowException, match="exceeds"):
        _operator(oversized).execute({})  # type: ignore[arg-type]


def test_metadata_only_carrier_never_returns_payload(tmp_path: Path) -> None:
    descriptor = json.loads((FIXTURES / "descriptor.json").read_text())
    descriptor["rights"]["permissions"] = ["ingest", "derive", "display"]
    path = tmp_path / "metadata-only.json"
    path.write_text(json.dumps(issue_evidence_carrier(**descriptor)))

    result = _operator(path).execute({})  # type: ignore[arg-type]
    assert result["disposition"] == "metadata_only"
    assert result["reason_codes"] == ["redistribution_not_permitted"]
    assert result["evidence"]["payload_disclosed"] is False
    assert "payload" not in result["evidence"]


def test_operator_has_no_template_or_shell_surface() -> None:
    assert VerifyEvidenceCarrierOperator.template_fields == ()
