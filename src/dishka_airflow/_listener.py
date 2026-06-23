"""Pluggy listener bridging the Airflow task lifecycle to a dishka container."""

import asyncio
from typing import Any

from airflow.listeners import hookimpl
from airflow.sdk import Context
from airflow.sdk.types import TaskInstance
from dishka import AsyncContainer, Container

from dishka_airflow._context import (
    REQUEST_ASYNC_CONTAINER,
    REQUEST_SYNC_CONTAINER,
)


class _DishkaListener:
    """Open a ``REQUEST`` scope per task run and close it when it finishes.

    The application-level container is stored on the instance; the per-task
    ``REQUEST`` container lives in one of the context variables defined in
    :mod:`dishka_airflow._context`, picked by the container kind.
    """

    def __init__(self, container: Container | AsyncContainer) -> None:
        self._container = container

    @hookimpl
    def on_task_instance_running(self, task_instance: TaskInstance) -> None:
        context = task_instance.get_template_context()
        provided: dict[Any, Any] = {
            Context: context,
            TaskInstance: task_instance,
        }
        request_container = self._container(context=provided)
        if isinstance(request_container, AsyncContainer):
            REQUEST_ASYNC_CONTAINER.set(request_container)
        else:
            REQUEST_SYNC_CONTAINER.set(request_container)

    @hookimpl
    def on_task_instance_success(self) -> None:
        self._close()

    @hookimpl
    def on_task_instance_failed(self) -> None:
        self._close()

    def _close(self) -> None:
        if isinstance(self._container, AsyncContainer):
            self._close_async()
        else:
            self._close_sync()

    @staticmethod
    def _close_sync() -> None:
        try:
            request_container = REQUEST_SYNC_CONTAINER.get()
        except LookupError:
            return
        request_container.close()

    @staticmethod
    def _close_async() -> None:
        try:
            request_container = REQUEST_ASYNC_CONTAINER.get()
        except LookupError:
            return
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(request_container.close())
        finally:
            loop.close()
