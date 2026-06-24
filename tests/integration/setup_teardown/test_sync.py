"""Integration: @setup/@teardown + @inject with a real ListenerManager (sync).

``@setup @inject def fn()`` stacks correctly because ``wrap_injection``
strips ``FromDishka`` params from ``__signature__`` before Airflow's
``python_task`` inspects the callable.  The underlying inject-wrapped
function is accessible via ``_TaskDecorator.function`` and is what Airflow
calls at runtime after ``on_task_instance_running`` opens a REQUEST scope.
"""

import pytest
from airflow.sdk import setup, teardown
from airflow.sdk.types import TaskInstance

from dishka_airflow import FromDishka, inject
from tests.common import REQUEST_DEP_VALUE, AppProvider, RequestDep
from tests.factories import make_task_instance
from tests.integration.conftest import sync_env


@pytest.mark.asyncio()
async def test_setup_injects_request_dependency(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @setup
    @inject
    def my_setup(dep: FromDishka[RequestDep]) -> str:
        return str(dep)

    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = my_setup.function()
        manager.hook.on_task_instance_success(
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == str(REQUEST_DEP_VALUE)
    app_provider.request_released.assert_called_once()


@pytest.mark.asyncio()
async def test_setup_failed_releases_request_scope(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @setup
    @inject
    def my_setup(dep: FromDishka[RequestDep]) -> str:
        return str(dep)

    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        my_setup.function()
        manager.hook.on_task_instance_failed(
            previous_state=None,
            task_instance=task_instance,
            error=RuntimeError("setup failed"),
        )

    app_provider.request_released.assert_called_once()


@pytest.mark.asyncio()
async def test_teardown_injects_request_dependency(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @teardown
    @inject
    def my_teardown(dep: FromDishka[RequestDep]) -> str:
        return str(dep)

    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = my_teardown.function()
        manager.hook.on_task_instance_success(
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == str(REQUEST_DEP_VALUE)
    app_provider.request_released.assert_called_once()


@pytest.mark.asyncio()
async def test_teardown_with_on_failure_flag(
    app_provider: AppProvider,
    task_instance: TaskInstance,
) -> None:
    @teardown(on_failure_fail_dagrun=True)
    @inject
    def my_teardown(dep: FromDishka[RequestDep]) -> str:
        return str(dep)

    assert my_teardown.on_failure_fail_dagrun is True

    async with sync_env(app_provider) as (_, manager):
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=task_instance,
        )
        result = my_teardown.function()
        manager.hook.on_task_instance_success(
            previous_state=None,
            task_instance=task_instance,
        )

    assert result == str(REQUEST_DEP_VALUE)


@pytest.mark.asyncio()
async def test_teardown_gets_fresh_scope_after_main_task_failure(
    app_provider: AppProvider,
) -> None:
    """Teardown is a separate TaskInstance with its own REQUEST scope.

    Both containers must resolve and release ``RequestDep`` independently,
    so ``request_released`` must be called exactly twice.
    """
    main_ti = make_task_instance("main_task")
    teardown_ti = make_task_instance("teardown_task")

    @inject
    def main_task(dep: FromDishka[RequestDep]) -> str:
        return str(dep)

    @teardown
    @inject
    def my_teardown(dep: FromDishka[RequestDep]) -> str:
        return str(dep)

    async with sync_env(app_provider) as (_, manager):
        # main task runs, resolves dep, then fails
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=main_ti,
        )
        assert main_task() == str(REQUEST_DEP_VALUE)
        manager.hook.on_task_instance_failed(
            previous_state=None,
            task_instance=main_ti,
            error=RuntimeError("main failed"),
        )

        # teardown gets its own fresh REQUEST scope
        manager.hook.on_task_instance_running(
            previous_state=None,
            task_instance=teardown_ti,
        )
        result = my_teardown.function()
        manager.hook.on_task_instance_success(
            previous_state=None,
            task_instance=teardown_ti,
        )

    assert result == str(REQUEST_DEP_VALUE)
    assert app_provider.request_released.call_count == 2
