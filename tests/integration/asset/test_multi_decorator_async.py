"""Integration: @asset.multi @inject decoration chain (async).

Mirrors test_multi_decorator_sync.py for async materialization functions.
"""

import asyncio

import pytest
from airflow.sdk import Context, asset
from airflow.sdk.definitions.asset import Asset
from airflow.sdk.types import TaskInstance

from dishka_airflow import FromDishka, inject
from tests.common import REQUEST_DEP_VALUE, AppProvider, RequestDep
from tests.integration.conftest import async_env

_OUTLET_A = Asset("multi_async_outlet_a")
_OUTLET_B = Asset("multi_async_outlet_b")


@asset.multi(schedule=None, outlets=[_OUTLET_A, _OUTLET_B])
@inject
async def async_multi_asset_request_dep(dep: FromDishka[RequestDep]) -> str:
    return str(dep)


@asset.multi(schedule=None, outlets=[_OUTLET_A, _OUTLET_B])
@inject
async def async_multi_asset_context(context: FromDishka[Context]) -> str:
    return context["ds"]


@asset.multi(schedule=None, outlets=[_OUTLET_A, _OUTLET_B])
@inject
async def async_multi_asset_task_instance(ti: FromDishka[TaskInstance]) -> TaskInstance:
    return ti


@asset.multi(schedule=None, outlets=[_OUTLET_A, _OUTLET_B])
@inject
async def async_multi_asset_for_failure(dep: FromDishka[RequestDep]) -> str:
    return str(dep)


@pytest.mark.asyncio()
async def test_async_multi_asset_callable_resolves_request_dependency(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    async with async_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = await async_multi_asset_request_dep._function()
        await asyncio.to_thread(
            manager.hook.on_task_instance_success,
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == str(REQUEST_DEP_VALUE)
    app_provider.request_released.assert_called_once()


@pytest.mark.asyncio()
async def test_async_multi_asset_callable_resolves_airflow_context(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    async with async_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = await async_multi_asset_context._function()
        await asyncio.to_thread(
            manager.hook.on_task_instance_success,
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == "2026-06-22"


@pytest.mark.asyncio()
async def test_async_multi_asset_callable_resolves_task_instance(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    async with async_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = await async_multi_asset_task_instance._function()
        await asyncio.to_thread(
            manager.hook.on_task_instance_success,
            previous_state=None,
            task_instance=task_instance,
        )

    assert result is task_instance


@pytest.mark.asyncio()
async def test_async_multi_asset_failed_releases_request_scope(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    async with async_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        await async_multi_asset_for_failure._function()
        await asyncio.to_thread(
            manager.hook.on_task_instance_failed,
            previous_state=None,
            task_instance=task_instance,
            error=RuntimeError("materialisation failed"),
        )

    app_provider.request_released.assert_called_once()
