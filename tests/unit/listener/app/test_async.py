"""APP scope lifecycle for the listener with an async container."""

import asyncio
from unittest.mock import Mock

import pytest
from airflow.sdk.types import TaskInstance

from dishka_airflow import FromDishka, inject
from tests.common import APP_DEP_VALUE, AppDep, AppMock, AppProvider
from tests.unit.conftest import create_airflow_env


@pytest.mark.asyncio()
async def test_app_dependency(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    async with create_airflow_env(
        app_provider,
        use_async_container=True,
    ) as (_, listener):
        listener.on_task_instance_running(task_instance=task_instance)

        @inject
        async def task(app_dep: FromDishka[AppDep], mock: FromDishka[Mock]) -> str:
            mock(app_dep)
            return "ok"

        assert await task() == "ok"
        await asyncio.to_thread(listener.on_task_instance_success)

        app_provider.mock.assert_called_with(APP_DEP_VALUE)
        app_provider.app_released.assert_not_called()

    app_provider.app_released.assert_called_once()


@pytest.mark.asyncio()
async def test_app_scope_reused_across_runs(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    app_mocks: list[AppMock] = []

    async with create_airflow_env(
        app_provider,
        use_async_container=True,
    ) as (_, listener):

        @inject
        async def task(app_mock: FromDishka[AppMock]) -> None:
            app_mocks.append(app_mock)

        for _ in range(2):
            listener.on_task_instance_running(task_instance=task_instance)
            await task()
            await asyncio.to_thread(listener.on_task_instance_success)

    assert len(app_mocks) == 2
    assert app_mocks[0] is app_mocks[1]
