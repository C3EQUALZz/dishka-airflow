"""Integration: @inject with asset materialization bodies (async)."""

import asyncio

import pytest
from airflow.sdk import Context
from airflow.sdk.types import TaskInstance

from dishka_airflow import FromDishka, inject
from tests.common import REQUEST_DEP_VALUE, AppProvider, RequestDep
from tests.integration.conftest import async_env


@pytest.mark.asyncio()
async def test_asset_body_injects_request_dependency(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @inject
    async def materialize(dep: FromDishka[RequestDep]) -> str:
        return str(dep)

    async with async_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = await materialize()
        await asyncio.to_thread(
            manager.hook.on_task_instance_success,
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == str(REQUEST_DEP_VALUE)
    app_provider.request_released.assert_called_once()


@pytest.mark.asyncio()
async def test_asset_body_injects_airflow_context(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @inject
    async def materialize(context: FromDishka[Context]) -> str:
        return context["ds"]

    async with async_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = await materialize()
        await asyncio.to_thread(
            manager.hook.on_task_instance_success,
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == "2026-06-22"


@pytest.mark.asyncio()
async def test_asset_body_with_inlet_and_injected_dep(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    """Inlet param stays in signature; dishka param is removed."""

    @inject
    async def materialize(inlet_data: str, dep: FromDishka[RequestDep]) -> str:
        return f"{inlet_data}:{dep}"

    async with async_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = await materialize(inlet_data="upstream")
        await asyncio.to_thread(
            manager.hook.on_task_instance_success,
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == f"upstream:{REQUEST_DEP_VALUE}"
