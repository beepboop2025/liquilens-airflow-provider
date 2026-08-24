"""Fail unless Airflow discovers the installed provider distribution."""

from __future__ import annotations

import json

from airflow.providers_manager import ProvidersManager

PACKAGE = "liquilens-airflow-provider"

manager = ProvidersManager()
if PACKAGE not in manager.providers:
    raise SystemExit(f"Airflow did not discover {PACKAGE}")
info, version = manager.providers[PACKAGE]
if version != "0.1.0":
    raise SystemExit(f"unexpected provider version: {version}")
if info.get("name") != "LiquiLens Evidence":
    raise SystemExit("unexpected provider metadata")
print(json.dumps({"package": PACKAGE, "version": version, "name": info["name"]}))
