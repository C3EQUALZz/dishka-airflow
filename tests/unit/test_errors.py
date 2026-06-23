"""Resolving dependencies without a matching open REQUEST scope."""

import pytest
from airflow.sdk.types import TaskInstance

from dishka_airflow import FromDishka, inject
from tests.common import AppProvider, RequestDep
from tests.unit.conftest import create_airflow_env


@pytest.mark.asyncio()
async def test_inject_without_running_hook_raises(
    app_provider: AppProvider,
) -> None:
    async with create_airflow_env(
        app_provider,
        use_async_container=False,
    ) as (_, _listener):

        @inject
        def task(request_dep: FromDishka[RequestDep]) -> str:
            return str(request_dep)

        with pytest.raises(LookupError):
            task()


@pytest.mark.asyncio()
async def test_sync_inject_with_async_scope_raises(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    async with create_airflow_env(
        app_provider,
        use_async_container=True,
    ) as (_, listener):
        listener.on_task_instance_running(task_instance=task_instance)

        @inject
        def task(request_dep: FromDishka[RequestDep]) -> str:
            return str(request_dep)

        with pytest.raises(LookupError):
            task()
