"""REQUEST scope lifecycle for the listener with an async container."""

import asyncio

import pytest
from airflow.sdk.types import TaskInstance

from dishka_airflow import FromDishka, inject
from tests.common import REQUEST_DEP_VALUE, AppProvider, RequestDep
from tests.unit.conftest import create_airflow_env


@pytest.mark.asyncio()
async def test_request_dependency_released_on_success(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    async with create_airflow_env(
        app_provider,
        use_async_container=True,
    ) as (_, listener):
        listener.on_task_instance_running(task_instance=task_instance)

        @inject
        async def task(request_dep: FromDishka[RequestDep]) -> str:
            return str(request_dep)

        assert await task() == str(REQUEST_DEP_VALUE)
        app_provider.request_released.assert_not_called()
        await asyncio.to_thread(listener.on_task_instance_success)
        app_provider.request_released.assert_called_once()


@pytest.mark.asyncio()
async def test_request_dependency_released_on_failure(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    async with create_airflow_env(
        app_provider,
        use_async_container=True,
    ) as (_, listener):
        listener.on_task_instance_running(task_instance=task_instance)

        @inject
        async def task(request_dep: FromDishka[RequestDep]) -> str:
            return str(request_dep)

        assert await task() == str(REQUEST_DEP_VALUE)
        await asyncio.to_thread(listener.on_task_instance_failed)
        app_provider.request_released.assert_called_once()


@pytest.mark.asyncio()
async def test_close_without_running_is_noop(app_provider: AppProvider) -> None:
    async with create_airflow_env(
        app_provider,
        use_async_container=True,
    ) as (_, listener):
        await asyncio.to_thread(listener.on_task_instance_success)
        await asyncio.to_thread(listener.on_task_instance_failed)

    app_provider.request_released.assert_not_called()
