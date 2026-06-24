"""DishkaPlugin wiring."""

import asyncio
import atexit
from unittest.mock import patch

from dishka import make_async_container, make_container

from dishka_airflow import AirflowProvider, DishkaPlugin
from dishka_airflow._listener import _DishkaListener
from tests.common import AppDep, AppProvider


def test_plugin_with_container_registers_listener() -> None:
    app_container = make_container(AirflowProvider())

    class _Plugin(DishkaPlugin):
        name = "test_dishka_plugin"
        container = app_container

    assert len(_Plugin.listeners) == 1
    assert isinstance(_Plugin.listeners[0], _DishkaListener)
    app_container.close()


def test_plugin_without_container_registers_no_listener() -> None:
    class _Plugin(DishkaPlugin):
        name = "empty_dishka_plugin"

    assert _Plugin.listeners == []


def test_async_plugin_registers_atexit_handler() -> None:
    with patch.object(atexit, "register") as mock_register:
        app_container = make_async_container(AirflowProvider())

        class _Plugin(DishkaPlugin):
            name = "test_async_atexit_plugin"
            container = app_container

        mock_register.assert_called_once()


def test_sync_plugin_does_not_register_atexit_handler() -> None:
    with patch.object(atexit, "register") as mock_register:
        app_container = make_container(AirflowProvider())

        class _Plugin(DishkaPlugin):
            name = "test_sync_no_atexit_plugin"
            container = app_container

        mock_register.assert_not_called()
        app_container.close()


def test_async_atexit_handler_calls_asyncio_run() -> None:
    with patch.object(atexit, "register") as mock_register:
        app_container = make_async_container(AirflowProvider())

        class _Plugin(DishkaPlugin):
            name = "test_async_close_plugin"
            container = app_container

        atexit_fn = mock_register.call_args[0][0]

    with patch("asyncio.run") as mock_run:
        atexit_fn()
        mock_run.assert_called_once()
        coro = mock_run.call_args[0][0]
        assert asyncio.iscoroutine(coro)
        coro.close()


def test_async_atexit_handler_releases_app_scope() -> None:
    """Atexit lambda must finalize APP-scope generators on process exit."""
    provider = AppProvider()

    with patch.object(atexit, "register") as mock_register:
        app_container = make_async_container(provider, AirflowProvider())

        class _Plugin(DishkaPlugin):
            name = "test_async_scope_plugin"
            container = app_container

        atexit_fn = mock_register.call_args[0][0]

    asyncio.run(app_container.get(AppDep))
    atexit_fn()

    provider.app_released.assert_called_once()


def test_async_atexit_handler_is_idempotent() -> None:
    """Calling the atexit handler twice must not raise."""
    with patch.object(atexit, "register") as mock_register:
        app_container = make_async_container(AirflowProvider())

        class _Plugin(DishkaPlugin):
            name = "test_async_idempotent_plugin"
            container = app_container

        atexit_fn = mock_register.call_args[0][0]

    atexit_fn()
    atexit_fn()
