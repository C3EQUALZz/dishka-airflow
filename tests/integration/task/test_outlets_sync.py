"""Integration: @task(outlets=[Asset(...)]) @inject decoration chain (sync).

A task that declares ``outlets`` is still a plain ``_TaskDecorator`` — the
``outlets`` list marks what assets it produces at runtime, but does not affect
how ``@inject`` strips ``FromDishka`` parameters or how dishka resolves
dependencies.  This file verifies that the two decorators compose cleanly when
``outlets`` is present.
"""

import inspect

import pytest
from airflow.sdk import Context
from airflow.sdk.definitions.asset import Asset
from airflow.sdk.definitions.decorators import task
from airflow.sdk.types import TaskInstance

from dishka_airflow import FromDishka, inject
from tests.common import REQUEST_DEP_VALUE, AppProvider, RequestDep
from tests.integration.conftest import sync_env

_OUTLET = Asset("s3://test-bucket/outlets-sync-output")


def test_outlets_task_signature_hides_dishka_params() -> None:
    @task(outlets=[_OUTLET])
    @inject
    def my_task(dep: FromDishka[RequestDep]) -> str:
        return str(dep)

    sig = inspect.signature(my_task.function)
    assert "dep" not in sig.parameters


def test_outlets_task_signature_preserves_xcom_param() -> None:
    @task(outlets=[_OUTLET])
    @inject
    def my_task(name: str, dep: FromDishka[RequestDep]) -> str:
        return f"{name}:{dep}"

    sig = inspect.signature(my_task.function)
    assert "name" in sig.parameters
    assert "dep" not in sig.parameters


@pytest.mark.asyncio()
async def test_outlets_task_resolves_request_dependency(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @task(outlets=[_OUTLET])
    @inject
    def my_task(dep: FromDishka[RequestDep]) -> str:
        return str(dep)

    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = my_task.function()
        manager.hook.on_task_instance_success(
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == str(REQUEST_DEP_VALUE)
    app_provider.request_released.assert_called_once()


@pytest.mark.asyncio()
async def test_outlets_task_resolves_airflow_context(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @task(outlets=[_OUTLET])
    @inject
    def my_task(context: FromDishka[Context]) -> str:
        return context["ds"]

    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = my_task.function()
        manager.hook.on_task_instance_success(
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == "2026-06-22"


@pytest.mark.asyncio()
async def test_outlets_task_xcom_param_alongside_injected_dep(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @task(outlets=[_OUTLET])
    @inject
    def my_task(name: str, dep: FromDishka[RequestDep]) -> str:
        return f"{name}:{dep}"

    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = my_task.function(name="airflow")
        manager.hook.on_task_instance_success(
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == f"airflow:{REQUEST_DEP_VALUE}"
    app_provider.request_released.assert_called_once()


@pytest.mark.asyncio()
async def test_outlets_task_failed_releases_request_scope(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @task(outlets=[_OUTLET])
    @inject
    def my_task(dep: FromDishka[RequestDep]) -> str:
        return str(dep)

    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        my_task.function()
        manager.hook.on_task_instance_failed(
            previous_state=None,
            task_instance=task_instance,
            error=RuntimeError("materialization failed"),
        )

    app_provider.request_released.assert_called_once()
