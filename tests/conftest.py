import pytest
from airflow.sdk.types import TaskInstance

from tests.common import AppProvider
from tests.factories import make_task_instance


@pytest.fixture()
def app_provider() -> AppProvider:
    return AppProvider()


@pytest.fixture()
def task_instance() -> TaskInstance:
    return make_task_instance()
