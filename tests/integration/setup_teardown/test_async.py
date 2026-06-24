"""Integration: @setup/@teardown + @inject with a real ListenerManager (async)."""

import asyncio

import pytest
from airflow.sdk import setup, teardown
from airflow.sdk.types import TaskInstance

from dishka_airflow import FromDishka, inject
from tests.common import REQUEST_DEP_VALUE, AppProvider, RequestDep
from tests.integration.conftest import async_env


@pytest.mark.asyncio()
async def test_setup_injects_request_dependency(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @setup
    @inject
    async def my_setup(dep: FromDishka[RequestDep]) -> str:
        return str(dep)

    async with async_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = await my_setup.function()
        await asyncio.to_thread(
            manager.hook.on_task_instance_success,
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == str(REQUEST_DEP_VALUE)
    app_provider.request_released.assert_called_once()


@pytest.mark.asyncio()
async def test_teardown_injects_request_dependency(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @teardown
    @inject
    async def my_teardown(dep: FromDishka[RequestDep]) -> str:
        return str(dep)

    async with async_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = await my_teardown.function()
        await asyncio.to_thread(
            manager.hook.on_task_instance_success,
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == str(REQUEST_DEP_VALUE)
    app_provider.request_released.assert_called_once()


@pytest.mark.asyncio()
async def test_teardown_with_on_failure_flag(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @teardown(on_failure_fail_dagrun=True)
    @inject
    async def my_teardown(dep: FromDishka[RequestDep]) -> str:
        return str(dep)

    assert my_teardown.on_failure_fail_dagrun is True

    async with async_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = await my_teardown.function()
        await asyncio.to_thread(
            manager.hook.on_task_instance_success,
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == str(REQUEST_DEP_VALUE)
