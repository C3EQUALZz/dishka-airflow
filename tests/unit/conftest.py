import inspect
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from dishka import (
    AsyncContainer,
    Container,
    make_async_container,
    make_container,
)

from dishka_airflow import AirflowProvider
from dishka_airflow._listener import _DishkaListener
from tests.common import AppProvider


async def _close_container(container: AsyncContainer | Container) -> None:
    result = container.close()
    if inspect.isawaitable(result):
        await result


@asynccontextmanager
async def create_airflow_env(
    provider: AppProvider,
    *,
    use_async_container: bool,
) -> AsyncIterator[tuple[AsyncContainer | Container, _DishkaListener]]:
    container: AsyncContainer | Container
    if use_async_container:
        container = make_async_container(provider, AirflowProvider())
    else:
        container = make_container(provider, AirflowProvider())

    listener = _DishkaListener(container)
    try:
        yield container, listener
    finally:
        await _close_container(container)
