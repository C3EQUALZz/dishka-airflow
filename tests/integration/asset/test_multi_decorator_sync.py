"""Integration: @asset.multi @inject decoration chain (sync).

``@asset.multi`` creates a ``MultiAssetDefinition`` with ``_function`` holding
the inject-wrapped callable — same mechanics as ``@asset``, but the decorator
emits multiple outlet assets.  Module-level definitions are required because
``@asset.multi`` also rejects nested functions (``f.__qualname__ != f.__name__``).
"""

import inspect

import pytest
from airflow.sdk import Context, asset
from airflow.sdk.definitions.asset import Asset
from airflow.sdk.types import TaskInstance

from dishka_airflow import FromDishka, inject
from tests.common import REQUEST_DEP_VALUE, AppProvider, RequestDep
from tests.integration.conftest import sync_env

_OUTLET_A = Asset("multi_sync_outlet_a")
_OUTLET_B = Asset("multi_sync_outlet_b")


@asset.multi(schedule=None, outlets=[_OUTLET_A, _OUTLET_B])
@inject
def sync_multi_asset_request_dep(dep: FromDishka[RequestDep]) -> None:
    _ = str(dep)


@asset.multi(schedule=None, outlets=[_OUTLET_A, _OUTLET_B])
@inject
def sync_multi_asset_context(context: FromDishka[Context]) -> str:
    return context["ds"]


@asset.multi(schedule=None, outlets=[_OUTLET_A, _OUTLET_B])
@inject
def sync_multi_asset_returning(dep: FromDishka[RequestDep]) -> str:
    return str(dep)


@asset.multi(schedule=None, outlets=[_OUTLET_A, _OUTLET_B])
@inject
def sync_multi_asset_for_failure(dep: FromDishka[RequestDep]) -> str:
    return str(dep)


def test_multi_asset_signature_hides_dishka_params() -> None:
    sig = inspect.signature(sync_multi_asset_request_dep._function)
    assert "dep" not in sig.parameters


def test_multi_asset_signature_is_empty_when_all_params_are_dishka() -> None:
    sig = inspect.signature(sync_multi_asset_returning._function)
    assert list(sig.parameters) == []


@pytest.mark.asyncio()
async def test_multi_asset_callable_resolves_request_dependency(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = sync_multi_asset_returning._function()
        manager.hook.on_task_instance_success(
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == str(REQUEST_DEP_VALUE)
    app_provider.request_released.assert_called_once()


@pytest.mark.asyncio()
async def test_multi_asset_callable_resolves_airflow_context(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = sync_multi_asset_context._function()
        manager.hook.on_task_instance_success(
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == "2026-06-22"


@pytest.mark.asyncio()
async def test_multi_asset_failed_releases_request_scope(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        sync_multi_asset_for_failure._function()
        manager.hook.on_task_instance_failed(
            previous_state=None,
            task_instance=task_instance,
            error=RuntimeError("materialisation failed"),
        )

    app_provider.request_released.assert_called_once()
