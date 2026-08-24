"""Offline verification of one local LiquiLens evidence carrier."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow.exceptions import AirflowException
from airflow.sdk import BaseOperator, Context
from liquilens_evidence import (
    EVIDENCE_CARRIER_MAX_BYTES,
    EvidenceCarrierError,
    verify_evidence_carrier,
)


class VerifyEvidenceCarrierOperator(BaseOperator):
    """Verify one explicitly selected local evidence carrier JSON file.

    The operator performs no network requests and invokes no shell. Rejected,
    malformed, oversized, unreadable, or tampered carriers fail the task. A
    metadata-only carrier succeeds with the payload removed by the core policy.
    The returned dictionary is JSON-serializable for Airflow XCom storage.

    ``carrier_path`` is deliberately not a template field. DAG authors must
    select the local file directly rather than interpolate an untrusted command.

    :param carrier_path: Path to one local carrier JSON file.
    :param evaluated_at: Optional timezone-aware verification instant. Defaults
        to the current UTC time in the core verifier.
    """

    template_fields: Sequence[str] = ()
    ui_color = "#d7f5ef"
    custom_operator_name = "Verify LiquiLens Evidence"

    def __init__(
        self,
        *,
        carrier_path: str,
        evaluated_at: datetime | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if not isinstance(carrier_path, str) or not carrier_path.strip():
            raise AirflowException("carrier_path must be a non-blank string")
        if evaluated_at is not None and (
            not isinstance(evaluated_at, datetime)
            or evaluated_at.tzinfo is None
            or evaluated_at.utcoffset() is None
        ):
            raise AirflowException("evaluated_at must be timezone-aware")
        self.carrier_path = carrier_path
        self.evaluated_at = evaluated_at

    @staticmethod
    def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError(f"duplicate JSON key: {key}")
            value[key] = item
        return value

    @staticmethod
    def _reject_non_finite(value: str) -> None:
        raise ValueError(f"non-finite JSON number: {value}")

    def _read_carrier(self) -> Mapping[str, Any]:
        path = Path(self.carrier_path).expanduser()
        try:
            resolved = path.resolve(strict=True)
            if not resolved.is_file():
                raise AirflowException("carrier_path must resolve to a regular file")
            with resolved.open("rb") as stream:
                encoded = stream.read(EVIDENCE_CARRIER_MAX_BYTES + 1)
        except AirflowException:
            raise
        except OSError as error:
            raise AirflowException("carrier file is missing or unreadable") from error

        if len(encoded) > EVIDENCE_CARRIER_MAX_BYTES:
            raise AirflowException(f"carrier exceeds {EVIDENCE_CARRIER_MAX_BYTES} encoded bytes")
        try:
            value = json.loads(
                encoded.decode("utf-8"),
                object_pairs_hook=self._unique_object,
                parse_constant=self._reject_non_finite,
            )
        except (UnicodeError, ValueError) as error:
            raise AirflowException("carrier file must contain strict UTF-8 JSON") from error
        if not isinstance(value, Mapping):
            raise AirflowException("carrier JSON root must be an object")
        return value

    def execute(self, context: Context) -> dict[str, Any]:
        """Verify the file and return only the policy-permitted view."""

        del context
        try:
            verified = verify_evidence_carrier(
                self._read_carrier(),
                evaluated_at=self.evaluated_at,
            )
            view = verified.export_view()
        except (EvidenceCarrierError, TypeError, ValueError) as error:
            raise AirflowException("evidence carrier verification failed") from error

        carrier = verified.carrier
        self.log.info(
            "Verified carrier_id=%s disposition=%s reasons=%s",
            carrier["carrier_id"],
            verified.disposition.value,
            ",".join(verified.reason_codes) or "none",
        )
        return {
            "carrier_id": carrier["carrier_id"],
            "record_hash": carrier["record_hash"],
            "disposition": verified.disposition.value,
            "reason_codes": list(verified.reason_codes),
            "policy_version": verified.policy_version,
            "evidence": view,
        }
