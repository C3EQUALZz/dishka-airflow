"""End-to-end task injection through a real Airflow ListenerManager (sync)."""

import pytest
from airflow.sdk import Context
from airflow.sdk.types import TaskInstance

from dishka_airflow import FromDishka, inject
from tests.common import REQUEST_DEP_VALUE, AppProvider, RequestDep
from tests.integration.conftest import sync_env


@pytest.mark.asyncio()
async def test_task_injects_request_dependency(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @inject
    def task(request_dep: FromDishka[RequestDep]) -> str:
        return str(request_dep)

    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = task()
        manager.hook.on_task_instance_success(
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == str(REQUEST_DEP_VALUE)
    app_provider.request_released.assert_called_once()


@pytest.mark.asyncio()
async def test_failed_hook_releases_request_scope(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @inject
    def task(request_dep: FromDishka[RequestDep]) -> str:
        return str(request_dep)

    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        assert task() == str(REQUEST_DEP_VALUE)
        manager.hook.on_task_instance_failed(
            previous_state=None,
            task_instance=task_instance,
            error=RuntimeError("boom"),
        )

    app_provider.request_released.assert_called_once()


@pytest.mark.asyncio()
async def test_task_injects_airflow_context(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @inject
    def task(context: FromDishka[Context]) -> str:
        return context["ds"]

    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = task()
        manager.hook.on_task_instance_success(
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == "2026-06-22"


@pytest.mark.asyncio()
async def test_task_injects_task_instance(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @inject
    def task(received: FromDishka[TaskInstance]) -> TaskInstance:
        return received

    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = task()
        manager.hook.on_task_instance_success(
            previous_state=None,
            task_instance=task_instance,
        )

    assert result is task_instance
