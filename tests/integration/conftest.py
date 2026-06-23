from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from airflow.sdk._shared.listeners.listener import ListenerManager
from airflow.sdk._shared.listeners.spec import lifecycle, taskinstance
from dishka import (
    AsyncContainer,
    Container,
    Provider,
    make_async_container,
    make_container,
)

from dishka_airflow import AirflowProvider
from dishka_airflow._listener import _DishkaListener

# Airflow's ListenerManager ships no type annotations, so it is used through
# an explicitly-Any alias to keep the strict-mode boundary honest.
_listener_manager_factory: Any = ListenerManager


def build_listener_manager(*listeners: object) -> Any:
    """Wire listeners into a real Airflow ``ListenerManager``.

    Registering through the genuine pluggy machinery validates that the
    ``@hookimpl`` signatures actually match Airflow's hookspecs -- a mismatch
    would otherwise be silently skipped at dispatch time.
    """
    manager = _listener_manager_factory()
    manager.add_hookspecs(lifecycle)
    manager.add_hookspecs(taskinstance)
    for listener in listeners:
        manager.add_listener(listener)
    return manager


@asynccontextmanager
async def async_env(
    *providers: Provider,
) -> AsyncIterator[tuple[AsyncContainer, Any]]:
    container = make_async_container(*providers, AirflowProvider())
    manager = build_listener_manager(_DishkaListener(container))
    try:
        yield container, manager
    finally:
        await container.close()


@asynccontextmanager
async def sync_env(
    *providers: Provider,
) -> AsyncIterator[tuple[Container, Any]]:
    container = make_container(*providers, AirflowProvider())
    manager = build_listener_manager(_DishkaListener(container))
    try:
        yield container, manager
    finally:
        container.close()
