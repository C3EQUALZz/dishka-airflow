"""Integration: @task @inject decoration chain (sync).

``@task @inject`` stacks correctly: ``@inject`` strips ``FromDishka`` params
from ``__signature__`` before ``@task`` sees the callable.  The inject-wrapped
function is accessible via ``_TaskDecorator.function`` — the same attribute
Airflow calls at task execution time.
"""

import inspect

import pytest
from airflow.sdk import Context
from airflow.sdk.definitions.decorators import task
from airflow.sdk.types import TaskInstance

from dishka_airflow import FromDishka, inject
from tests.common import REQUEST_DEP_VALUE, AppProvider, RequestDep
from tests.integration.conftest import sync_env


def test_signature_hides_dishka_params() -> None:
    @task
    @inject
    def my_task(dep: FromDishka[RequestDep]) -> str:
        return str(dep)

    sig = inspect.signature(my_task.function)
    assert "dep" not in sig.parameters


def test_signature_is_empty_when_all_params_are_dishka() -> None:
    @task
    @inject
    def my_task(dep: FromDishka[RequestDep], ctx: FromDishka[Context]) -> str:
        return str(dep)

    sig = inspect.signature(my_task.function)
    assert list(sig.parameters) == []


@pytest.mark.asyncio()
async def test_task_decorator_resolves_request_dependency(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @task
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
async def test_task_decorator_resolves_airflow_context(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @task
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
async def test_task_decorator_resolves_task_instance(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @task
    @inject
    def my_task(ti: FromDishka[TaskInstance]) -> TaskInstance:
        return ti

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

    assert result is task_instance


@pytest.mark.asyncio()
async def test_task_decorator_failed_releases_request_scope(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @task
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
            error=RuntimeError("boom"),
        )

    app_provider.request_released.assert_called_once()
