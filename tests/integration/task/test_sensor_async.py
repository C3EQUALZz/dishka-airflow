"""Integration: @task.sensor + @inject (async)."""

import asyncio

import pytest
from airflow.sdk import Context
from airflow.sdk.definitions.decorators import task
from airflow.sdk.types import TaskInstance

from dishka_airflow import FromDishka, inject
from tests.common import AppProvider, RequestDep
from tests.integration.conftest import async_env


@pytest.mark.asyncio()
async def test_async_sensor_resolves_request_dependency(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @task.sensor
    @inject
    async def my_sensor(dep: FromDishka[RequestDep]) -> bool:
        _ = str(dep)
        return True

    async with async_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = await my_sensor.function()
        await asyncio.to_thread(
            manager.hook.on_task_instance_success,
            previous_state=None,
            task_instance=task_instance,
        )

    assert result is True
    app_provider.request_released.assert_called_once()


@pytest.mark.asyncio()
async def test_async_sensor_resolves_airflow_context(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @task.sensor
    @inject
    async def my_sensor(context: FromDishka[Context]) -> str:
        return context["ds"]

    async with async_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = await my_sensor.function()
        await asyncio.to_thread(
            manager.hook.on_task_instance_success,
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == "2026-06-22"


@pytest.mark.asyncio()
async def test_async_sensor_failed_releases_request_scope(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @task.sensor
    @inject
    async def my_sensor(dep: FromDishka[RequestDep]) -> bool:
        _ = str(dep)
        return False

    async with async_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        await my_sensor.function()
        await asyncio.to_thread(
            manager.hook.on_task_instance_failed,
            previous_state=None,
            task_instance=task_instance,
            error=RuntimeError("sensor failed"),
        )

    app_provider.request_released.assert_called_once()
