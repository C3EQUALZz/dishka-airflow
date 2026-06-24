"""Integration: @task.short_circuit + @inject (sync)."""

import inspect

import pytest
from airflow.sdk import Context
from airflow.sdk.definitions.decorators import task
from airflow.sdk.types import TaskInstance

from dishka_airflow import FromDishka, inject
from tests.common import AppProvider, RequestDep
from tests.integration.conftest import sync_env


def test_short_circuit_signature_hides_dishka_params() -> None:
    @task.short_circuit
    @inject
    def my_short_circuit(dep: FromDishka[RequestDep]) -> bool:
        return True

    sig = inspect.signature(my_short_circuit.function)
    assert "dep" not in sig.parameters


@pytest.mark.asyncio()
async def test_short_circuit_resolves_request_dependency(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @task.short_circuit
    @inject
    def my_short_circuit(dep: FromDishka[RequestDep]) -> bool:
        _ = str(dep)
        return True

    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = my_short_circuit.function()
        manager.hook.on_task_instance_success(
            previous_state=None,
            task_instance=task_instance,
        )

    assert result is True
    app_provider.request_released.assert_called_once()


@pytest.mark.asyncio()
async def test_short_circuit_resolves_airflow_context(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @task.short_circuit
    @inject
    def my_short_circuit(context: FromDishka[Context]) -> str:
        return context["ds"]

    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = my_short_circuit.function()
        manager.hook.on_task_instance_success(
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == "2026-06-22"


@pytest.mark.asyncio()
async def test_short_circuit_failed_releases_request_scope(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @task.short_circuit
    @inject
    def my_short_circuit(dep: FromDishka[RequestDep]) -> bool:
        _ = str(dep)
        return False

    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        my_short_circuit.function()
        manager.hook.on_task_instance_failed(
            previous_state=None,
            task_instance=task_instance,
            error=RuntimeError("short circuit"),
        )

    app_provider.request_released.assert_called_once()
