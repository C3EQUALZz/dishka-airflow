"""Integration: full @asset @inject decoration chain (sync).

``@asset`` rejects nested functions (``f.__qualname__ != f.__name__``), so
asset definitions must live at module level.  Tests reference these
module-level ``AssetDefinition`` objects and call ``._function`` (the
inject-wrapped callable) after the listener opens a REQUEST scope.

Key invariants verified:
- ``@inject`` hides ``FromDishka`` params so ``@asset`` does not treat
  them as upstream inlet dependencies.
- The ``python_callable`` stored in ``AssetDefinition._function`` resolves
  all dishka deps when called with a REQUEST scope active.
"""

import inspect

import pytest
from airflow.sdk import Context, asset
from airflow.sdk.types import TaskInstance

from dishka_airflow import FromDishka, inject
from tests.common import REQUEST_DEP_VALUE, AppProvider, RequestDep
from tests.integration.conftest import sync_env


@asset(schedule=None)
@inject
def sync_asset_request_dep(dep: FromDishka[RequestDep]) -> str:
    return str(dep)


@asset(schedule=None)
@inject
def sync_asset_context(context: FromDishka[Context]) -> str:
    return context["ds"]


@asset(schedule=None)
@inject
def sync_asset_task_instance(ti: FromDishka[TaskInstance]) -> TaskInstance:
    return ti


@asset(schedule=None)
@inject
def sync_asset_inlet_with_dep(inlet_data: str, dep: FromDishka[RequestDep]) -> str:
    return f"{inlet_data}:{dep}"


@asset(schedule=None)
@inject
def sync_asset_multi_deps(
    dep: FromDishka[RequestDep],
    context: FromDishka[Context],
) -> str:
    return f"{dep}|{context['ds']}"


@asset(schedule=None)
@inject
def sync_asset_for_failure(dep: FromDishka[RequestDep]) -> str:
    return str(dep)


def test_signature_hides_single_dishka_param() -> None:
    sig = inspect.signature(sync_asset_request_dep._function)
    assert "dep" not in sig.parameters


def test_signature_is_empty_when_all_params_are_dishka() -> None:
    sig = inspect.signature(sync_asset_multi_deps._function)
    assert list(sig.parameters) == []


def test_inlet_param_stays_in_signature() -> None:
    sig = inspect.signature(sync_asset_inlet_with_dep._function)
    assert "inlet_data" in sig.parameters
    assert "dep" not in sig.parameters


@pytest.mark.asyncio()
async def test_callable_resolves_request_dependency(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = sync_asset_request_dep._function()
        manager.hook.on_task_instance_success(
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == str(REQUEST_DEP_VALUE)
    app_provider.request_released.assert_called_once()


@pytest.mark.asyncio()
async def test_callable_resolves_airflow_context(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = sync_asset_context._function()
        manager.hook.on_task_instance_success(
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == "2026-06-22"


@pytest.mark.asyncio()
async def test_callable_resolves_task_instance(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = sync_asset_task_instance._function()
        manager.hook.on_task_instance_success(
            previous_state=None,
            task_instance=task_instance,
        )

    assert result is task_instance


@pytest.mark.asyncio()
async def test_inlet_with_injected_dep(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = sync_asset_inlet_with_dep._function(inlet_data="upstream")
        manager.hook.on_task_instance_success(
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == f"upstream:{REQUEST_DEP_VALUE}"


@pytest.mark.asyncio()
async def test_failed_task_releases_request_scope(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        sync_asset_for_failure._function()
        manager.hook.on_task_instance_failed(
            previous_state=None,
            task_instance=task_instance,
            error=RuntimeError("materialisation failed"),
        )

    app_provider.request_released.assert_called_once()


@pytest.mark.asyncio()
async def test_multiple_dishka_deps_all_resolved(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = sync_asset_multi_deps._function()
        manager.hook.on_task_instance_success(
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == f"{REQUEST_DEP_VALUE}|2026-06-22"
