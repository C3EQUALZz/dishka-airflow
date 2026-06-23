"""inject decorator behaviour with a sync container."""

import pytest
from airflow.sdk.types import TaskInstance

from dishka_airflow import FromDishka, inject
from tests.common import REQUEST_DEP_VALUE, AppProvider, RequestDep
from tests.unit.conftest import create_airflow_env


def test_inject_without_dependencies_is_passthrough() -> None:
    @inject
    def task(value: int) -> int:
        return value + 1

    assert task(41) == 42


@pytest.mark.asyncio()
async def test_inject_keeps_regular_arguments(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    async with create_airflow_env(
        app_provider,
        use_async_container=False,
    ) as (_, listener):
        listener.on_task_instance_running(task_instance=task_instance)

        @inject
        def task(value: int, request_dep: FromDishka[RequestDep]) -> str:
            return f"{value}-{request_dep}"

        assert task(42) == f"42-{REQUEST_DEP_VALUE}"
        listener.on_task_instance_success()
