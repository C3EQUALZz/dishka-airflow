"""Integration: full @asset @inject decoration chain (async).

Mirrors test_decorator_sync.py for async materialization functions.
Asset definitions at module level (see test_decorator_sync.py for explanation).
"""

import asyncio

import pytest
from airflow.sdk import Context, asset
from airflow.sdk.types import TaskInstance

from dishka_airflow import FromDishka, inject
from tests.common import REQUEST_DEP_VALUE, AppProvider, RequestDep
from tests.integration.conftest import async_env


@asset(schedule=None)
@inject
async def async_asset_request_dep(dep: FromDishka[RequestDep]) -> str:
    return str(dep)


@asset(schedule=None)
@inject
async def async_asset_context(context: FromDishka[Context]) -> str:
    return context["ds"]


@asset(schedule=None)
@inject
async def async_asset_task_instance(ti: FromDishka[TaskInstance]) -> TaskInstance:
    return ti


@asset(schedule=None)
@inject
async def async_asset_inlet_with_dep(inlet_data: str, dep: FromDishka[RequestDep]) -> str:
    return f"{inlet_data}:{dep}"


@asset(schedule=None)
@inject
async def async_asset_multi_deps(
    dep: FromDishka[RequestDep],
    context: FromDishka[Context],
) -> str:
    return f"{dep}|{context['ds']}"


@asset(schedule=None)
@inject
async def async_asset_for_failure(dep: FromDishka[RequestDep]) -> str:
    return str(dep)


@pytest.mark.asyncio()
async def test_async_callable_resolves_request_dependency(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    async with async_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = await async_asset_request_dep._function()
        await asyncio.to_thread(
            manager.hook.on_task_instance_success,
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == str(REQUEST_DEP_VALUE)
    app_provider.request_released.assert_called_once()


@pytest.mark.asyncio()
async def test_async_callable_resolves_airflow_context(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    async with async_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = await async_asset_context._function()
        await asyncio.to_thread(
            manager.hook.on_task_instance_success,
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == "2026-06-22"


@pytest.mark.asyncio()
async def test_async_callable_resolves_task_instance(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    async with async_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = await async_asset_task_instance._function()
        await asyncio.to_thread(
            manager.hook.on_task_instance_success,
            previous_state=None,
            task_instance=task_instance,
        )

    assert result is task_instance


@pytest.mark.asyncio()
async def test_async_inlet_with_injected_dep(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    async with async_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = await async_asset_inlet_with_dep._function(inlet_data="upstream")
        await asyncio.to_thread(
            manager.hook.on_task_instance_success,
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == f"upstream:{REQUEST_DEP_VALUE}"


@pytest.mark.asyncio()
async def test_async_failed_task_releases_request_scope(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    async with async_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        await async_asset_for_failure._function()
        await asyncio.to_thread(
            manager.hook.on_task_instance_failed,
            previous_state=None,
            task_instance=task_instance,
            error=RuntimeError("materialisation failed"),
        )

    app_provider.request_released.assert_called_once()


@pytest.mark.asyncio()
async def test_async_multiple_dishka_deps_all_resolved(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    async with async_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = await async_asset_multi_deps._function()
        await asyncio.to_thread(
            manager.hook.on_task_instance_success,
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == f"{REQUEST_DEP_VALUE}|2026-06-22"
