"""Fixtures for asset decorator integration tests."""

from collections.abc import Iterator

import pytest
from airflow.sdk import DAG


@pytest.fixture()
def dag() -> Iterator[DAG]:
    """Isolated DAG context for tests that inspect task/operator structure.

    Uses ``auto_register=False`` so the DAG is not added to the global
    dag bag, keeping test runs hermetic.
    """
    with DAG(
        dag_id="test_asset_dag", schedule=None, catchup=False, auto_register=False
    ) as d:
        yield d
