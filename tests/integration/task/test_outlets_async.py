"""Integration: @task(outlets=[Asset(...)]) @inject decoration chain (async).

Mirrors test_outlets_sync.py for async task functions.
"""

import asyncio

import pytest
from airflow.sdk import Context
from airflow.sdk.definitions.asset import Asset
from airflow.sdk.definitions.decorators import task
from airflow.sdk.types import TaskInstance

from dishka_airflow import FromDishka, inject
from tests.common import REQUEST_DEP_VALUE, AppProvider, RequestDep
from tests.integration.conftest import async_env

_OUTLET = Asset("s3://test-bucket/outlets-async-output")


@pytest.mark.asyncio()
async def test_async_outlets_task_resolves_request_dependency(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @task(outlets=[_OUTLET])
    @inject
    async def my_task(dep: FromDishka[RequestDep]) -> str:
        return str(dep)

    async with async_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = await my_task.function()
        await asyncio.to_thread(
            manager.hook.on_task_instance_success,
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == str(REQUEST_DEP_VALUE)
    app_provider.request_released.assert_called_once()


@pytest.mark.asyncio()
async def test_async_outlets_task_resolves_airflow_context(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @task(outlets=[_OUTLET])
    @inject
    async def my_task(context: FromDishka[Context]) -> str:
        return context["ds"]

    async with async_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = await my_task.function()
        await asyncio.to_thread(
            manager.hook.on_task_instance_success,
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == "2026-06-22"


@pytest.mark.asyncio()
async def test_async_outlets_task_xcom_param_alongside_injected_dep(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @task(outlets=[_OUTLET])
    @inject
    async def my_task(name: str, dep: FromDishka[RequestDep]) -> str:
        return f"{name}:{dep}"

    async with async_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = await my_task.function(name="airflow")
        await asyncio.to_thread(
            manager.hook.on_task_instance_success,
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == f"airflow:{REQUEST_DEP_VALUE}"
    app_provider.request_released.assert_called_once()


@pytest.mark.asyncio()
async def test_async_outlets_task_failed_releases_request_scope(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @task(outlets=[_OUTLET])
    @inject
    async def my_task(dep: FromDishka[RequestDep]) -> str:
        return str(dep)

    async with async_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        await my_task.function()
        await asyncio.to_thread(
            manager.hook.on_task_instance_failed,
            previous_state=None,
            task_instance=task_instance,
            error=RuntimeError("materialization failed"),
        )

    app_provider.request_released.assert_called_once()
