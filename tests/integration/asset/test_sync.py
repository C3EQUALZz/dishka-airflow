"""Integration: @inject with asset materialization bodies (sync).

``@asset`` creates DAG-level constructs at decoration time, so we cannot
apply it directly in a test without a full Airflow DAG context.  Instead we
test the materialisation body decorated with ``@inject`` the same way
``_AssetMainOperator`` would call it at runtime: after
``on_task_instance_running`` opens a REQUEST scope, it calls the
``python_callable`` (= the inject-wrapped function) with only the
non-``FromDishka`` kwargs it found in the signature.
"""

import pytest
from airflow.sdk import Context
from airflow.sdk.types import TaskInstance

from dishka_airflow import FromDishka, inject
from tests.common import REQUEST_DEP_VALUE, AppProvider, RequestDep
from tests.integration.conftest import sync_env


@pytest.mark.asyncio()
async def test_asset_body_injects_request_dependency(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @inject
    def materialize(dep: FromDishka[RequestDep]) -> str:
        return str(dep)

    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = materialize()
        manager.hook.on_task_instance_success(
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
    def materialize(context: FromDishka[Context]) -> str:
        return context["ds"]

    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = materialize()
        manager.hook.on_task_instance_success(
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == "2026-06-22"


@pytest.mark.asyncio()
async def test_asset_body_with_inlet_and_injected_dep(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    """Inlet param (no default) stays in signature; dishka param is removed."""

    @inject
    def materialize(inlet_data: str, dep: FromDishka[RequestDep]) -> str:
        return f"{inlet_data}:{dep}"

    # _AssetMainOperator would pass only params remaining in __signature__
    # (inlet_data), dishka resolves dep from the REQUEST container.
    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = materialize(inlet_data="upstream")
        manager.hook.on_task_instance_success(
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == f"upstream:{REQUEST_DEP_VALUE}"


@pytest.mark.asyncio()
async def test_asset_body_failed_releases_request_scope(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @inject
    def materialize(dep: FromDishka[RequestDep]) -> str:
        return str(dep)

    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        materialize()
        manager.hook.on_task_instance_failed(
            previous_state=None,
            task_instance=task_instance,
            error=RuntimeError("materialisation failed"),
        )

    app_provider.request_released.assert_called_once()
