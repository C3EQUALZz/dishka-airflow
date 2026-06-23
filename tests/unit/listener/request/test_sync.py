"""REQUEST scope lifecycle for the listener with a sync container."""

import pytest
from airflow.sdk.types import TaskInstance

from dishka_airflow import FromDishka, inject
from tests.common import REQUEST_DEP_VALUE, AppProvider, RequestDep
from tests.unit.conftest import create_airflow_env


@pytest.mark.asyncio()
async def test_request_dependency_released_on_success(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    async with create_airflow_env(
        app_provider,
        use_async_container=False,
    ) as (_, listener):
        listener.on_task_instance_running(task_instance=task_instance)

        @inject
        def task(request_dep: FromDishka[RequestDep]) -> str:
            return str(request_dep)

        assert task() == str(REQUEST_DEP_VALUE)
        app_provider.request_released.assert_not_called()
        listener.on_task_instance_success()
        app_provider.request_released.assert_called_once()


@pytest.mark.asyncio()
async def test_request_dependency_released_on_failure(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    async with create_airflow_env(
        app_provider,
        use_async_container=False,
    ) as (_, listener):
        listener.on_task_instance_running(task_instance=task_instance)

        @inject
        def task(request_dep: FromDishka[RequestDep]) -> str:
            return str(request_dep)

        assert task() == str(REQUEST_DEP_VALUE)
        listener.on_task_instance_failed()
        app_provider.request_released.assert_called_once()


@pytest.mark.asyncio()
async def test_request_scope_fresh_per_run(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    async with create_airflow_env(
        app_provider,
        use_async_container=False,
    ) as (_, listener):

        @inject
        def task(request_dep: FromDishka[RequestDep]) -> str:
            return str(request_dep)

        for _ in range(2):
            listener.on_task_instance_running(task_instance=task_instance)
            assert task() == str(REQUEST_DEP_VALUE)
            listener.on_task_instance_success()

    assert app_provider.request_released.call_count == 2


@pytest.mark.asyncio()
async def test_close_without_running_is_noop(app_provider: AppProvider) -> None:
    async with create_airflow_env(
        app_provider,
        use_async_container=False,
    ) as (_, listener):
        listener.on_task_instance_success()
        listener.on_task_instance_failed()

    app_provider.request_released.assert_not_called()
