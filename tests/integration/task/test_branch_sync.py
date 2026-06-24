"""Integration: @task.branch + @inject (sync)."""

import inspect

import pytest
from airflow.sdk import Context
from airflow.sdk.definitions.decorators import task
from airflow.sdk.types import TaskInstance

from dishka_airflow import FromDishka, inject
from tests.common import AppProvider, RequestDep
from tests.integration.conftest import sync_env


def test_branch_signature_hides_dishka_params() -> None:
    @task.branch
    @inject
    def my_branch(dep: FromDishka[RequestDep]) -> str:
        return "task_a"

    sig = inspect.signature(my_branch.function)
    assert "dep" not in sig.parameters


@pytest.mark.asyncio()
async def test_branch_resolves_request_dependency(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @task.branch
    @inject
    def my_branch(dep: FromDishka[RequestDep]) -> str:
        _ = str(dep)
        return "task_a"

    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = my_branch.function()
        manager.hook.on_task_instance_success(
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == "task_a"
    app_provider.request_released.assert_called_once()


@pytest.mark.asyncio()
async def test_branch_resolves_airflow_context(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @task.branch
    @inject
    def my_branch(context: FromDishka[Context]) -> str:
        return context["ds"]

    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = my_branch.function()
        manager.hook.on_task_instance_success(
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == "2026-06-22"


@pytest.mark.asyncio()
async def test_branch_failed_releases_request_scope(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @task.branch
    @inject
    def my_branch(dep: FromDishka[RequestDep]) -> str:
        return str(dep)

    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        my_branch.function()
        manager.hook.on_task_instance_failed(
            previous_state=None,
            task_instance=task_instance,
            error=RuntimeError("branch failed"),
        )

    app_provider.request_released.assert_called_once()
