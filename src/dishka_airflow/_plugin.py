"""Airflow plugin that wires a dishka container into task execution."""

import asyncio
import atexit
from typing import Any

from airflow.sdk.plugins_manager import AirflowPlugin
from dishka import AsyncContainer, Container

from dishka_airflow._listener import _DishkaListener


def _close_container(container: AsyncContainer) -> None:
    def _close() -> None:
        asyncio.run(container.close())

    atexit.register(_close)


class DishkaPlugin(AirflowPlugin):
    """Airflow plugin that registers the dishka task lifecycle listener.

    Subclass it in your ``plugins/`` folder and assign a container::

        from dishka import make_container
        from dishka_airflow import DishkaPlugin
        from myapp.providers import MyProvider

        class MyDishkaPlugin(DishkaPlugin):
            name = "my_dishka_plugin"
            container = make_container(MyProvider())

    The container is read at subclass-definition time to build the listener.
    For async containers an ``atexit`` handler is registered to ensure async
    providers (e.g. ``AsyncEngine``, S3 clients) are properly disposed when
    the task subprocess exits.
    """

    name = "dishka_plugin"
    container: Container | AsyncContainer | None = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if cls.container is not None:
            cls.listeners = [_DishkaListener(cls.container)]
            if isinstance(cls.container, AsyncContainer):
                _close_container(cls.container)
