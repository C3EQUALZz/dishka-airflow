"""End-to-end task injection through a real Airflow ListenerManager (async)."""

import asyncio

import pytest
from airflow.sdk.types import TaskInstance

from dishka_airflow import FromDishka, inject
from tests.common import REQUEST_DEP_VALUE, AppProvider, RequestDep
from tests.integration.conftest import async_env


@pytest.mark.asyncio()
async def test_task_injects_request_dependency(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @inject
    async def task(request_dep: FromDishka[RequestDep]) -> str:
        return str(request_dep)

    async with async_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = await task()
        await asyncio.to_thread(
            manager.hook.on_task_instance_success,
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == str(REQUEST_DEP_VALUE)
    app_provider.request_released.assert_called_once()
