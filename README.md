# LiquiLens Airflow Provider

An Apache Airflow 3 provider for offline, fail-closed verification of one local
[LiquiLens Evidence Carrier](https://liquilens.in/protocol/) JSON file per task.

The provider does not collect data, make network requests, invoke a shell, make
recommendations, assign credit ratings, or execute financial actions. It only
returns the view allowed by the carrier's verified clocks and rights policy.

## Install

Release artifacts are published on GitHub, not PyPI. The provider metadata pins
the public `liquilens-evidence` v0.14.0 wheel by URL and SHA-256. To verify that
core artifact independently:

```shell
python -m pip install --require-hashes \
  -r https://raw.githubusercontent.com/beepboop2025/liquilens-airflow-provider/v0.1.0/constraints/liquilens-evidence-v0.14.0.txt
```

The pinned core wheel is:

- URL: `https://github.com/beepboop2025/liquilens-evidence-carrier/releases/download/v0.14.0/liquilens_evidence-0.14.0-py3-none-any.whl`
- SHA-256: `f0162affab57307c8e20acf91dcefc33840f91e8cf9969a8d5ec8d8df860cd24`

The tested runtime is Apache Airflow 3.3.1 on Python 3.11. The package declares
Airflow `>=3.0,<4` and Python `>=3.11,<3.15`.

## DAG usage

```python
from datetime import UTC, datetime

from airflow.sdk import DAG
from liquilens_airflow_provider import VerifyEvidenceCarrierOperator

with DAG(
    dag_id="verify_local_evidence",
    schedule=None,
    start_date=datetime(2026, 8, 24, tzinfo=UTC),
    catchup=False,
):
    VerifyEvidenceCarrierOperator(
        task_id="verify_evidence",
        carrier_path="/opt/airflow/evidence/carrier.json",
    )
```

`carrier_path` is not a Jinja template and is never passed to a shell. The
operator reads at most 1 MiB, requires strict UTF-8 JSON without duplicate keys,
and calls the public v0.14 verifier directly.

The task fails for missing, unreadable, oversized, malformed, tampered, unsafe,
not-yet-known, or policy-rejected carriers. A valid metadata-only carrier can
succeed, but its payload is removed before the JSON-serializable result reaches
XCom. A fully allowed and current carrier returns its verified payload.

## Provider discovery

The standard `apache_airflow_provider` entry point exposes the operator to
Airflow. After installation:

```shell
airflow providers list | grep liquilens-airflow-provider
```

Hosted CI validates entry-point discovery and executes the committed DAG with a
real Airflow task and metadata database. Release CI repeats those gates, builds
the wheel and source distribution from a signed commit/tag, attests both, then
downloads the public wheel without credentials into a clean consumer job and
runs the DAG again.

## Security and scope

- No credentials or remote services are required.
- No network client or subprocess API is used by the operator.
- The all-false financial-authority boundary comes from the verified carrier.
- Errors do not log carrier payloads.
- Security reports go through [private vulnerability reporting](SECURITY.md).

Apache Airflow is a trademark of the Apache Software Foundation. This is an
independent third-party provider and is not an Apache Software Foundation
release or an Apache community-managed provider.
