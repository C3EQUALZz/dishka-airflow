"""The :func:`inject` decorator resolving dependencies inside Airflow tasks."""

from collections.abc import Awaitable, Callable
from inspect import iscoroutinefunction
from typing import Any, overload

from dishka.integrations.base import wrap_injection

from dishka_airflow._context import (
    REQUEST_ASYNC_CONTAINER,
    REQUEST_SYNC_CONTAINER,
)
from dishka_airflow._types import ParamsP, ReturnT


def _inject_sync(
    func: Callable[ParamsP, ReturnT],
) -> Callable[..., ReturnT]:
    return wrap_injection(
        func=func,
        container_getter=lambda _args, _kwargs: REQUEST_SYNC_CONTAINER.get(),
        is_async=False,
        manage_scope=False,
    )


def _inject_async(
    func: Callable[ParamsP, Awaitable[ReturnT]],
) -> Callable[..., Awaitable[ReturnT]]:
    return wrap_injection(
        func=func,
        container_getter=lambda _args, _kwargs: REQUEST_ASYNC_CONTAINER.get(),
        is_async=True,
        manage_scope=False,
    )


@overload
def inject(
    func: Callable[..., Awaitable[ReturnT]],
) -> Callable[..., Awaitable[ReturnT]]: ...


@overload
def inject(func: Callable[..., ReturnT]) -> Callable[..., ReturnT]: ...


def inject(func: Callable[ParamsP, Any]) -> Callable[..., Any]:
    """Inject dishka dependencies into an Airflow task callable.

    Dispatches between synchronous and asynchronous tasks automatically and
    resolves dependencies from the ``REQUEST`` container opened by the
    listener (``manage_scope=False``).
    """
    if iscoroutinefunction(func):
        return _inject_async(func)
    return _inject_sync(func)
