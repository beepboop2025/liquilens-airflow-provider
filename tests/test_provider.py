from __future__ import annotations

from importlib.metadata import entry_points

from airflow.providers_manager import ProvidersManager

from liquilens_airflow_provider.get_provider_info import get_provider_info


def test_standard_provider_entry_point_loads() -> None:
    matches = [
        item
        for item in entry_points(group="apache_airflow_provider")
        if item.name == "provider_info"
        and item.value == "liquilens_airflow_provider.get_provider_info:get_provider_info"
    ]
    assert len(matches) == 1
    assert matches[0].load()() == get_provider_info()


def test_airflow_provider_manager_discovers_distribution() -> None:
    manager = ProvidersManager()
    assert "liquilens-airflow-provider" in manager.providers
    provider = manager.providers["liquilens-airflow-provider"]
    assert provider.version == "0.1.0"
    assert provider.data["name"] == "LiquiLens Evidence"
    assert provider.data["operators"][0]["python-modules"] == [
        "liquilens_airflow_provider.operators.evidence"
    ]
