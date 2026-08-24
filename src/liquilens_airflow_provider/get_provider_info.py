"""Airflow provider discovery metadata."""

from __future__ import annotations

from typing import Any


def get_provider_info() -> dict[str, Any]:
    """Return metadata consumed by Airflow's apache_airflow_provider entry point."""

    return {
        "package-name": "liquilens-airflow-provider",
        "name": "LiquiLens Evidence",
        "description": (
            "Offline, fail-closed verification and rights-bounded projection "
            "of local LiquiLens financial evidence carrier JSON."
        ),
        "integrations": [
            {
                "integration-name": "LiquiLens Evidence",
                "external-doc-url": "https://liquilens.in/protocol/",
                "tags": ["finance", "evidence", "provenance", "offline"],
            }
        ],
        "operators": [
            {
                "integration-name": "LiquiLens Evidence",
                "python-modules": [
                    "liquilens_airflow_provider.operators.evidence",
                ],
            }
        ],
    }
