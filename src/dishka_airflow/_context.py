"""Context-local storage for the per-task ``REQUEST`` container.

Synchronous and asynchronous containers are kept in separate context
variables so that the value type stays precise (no ``Container | AsyncContainer``
union and no casts at the resolution site).
"""

from contextvars import ContextVar
from typing import Final

from dishka import AsyncContainer, Container

REQUEST_SYNC_CONTAINER: Final[ContextVar[Container]] = ContextVar(
    "dishka_airflow_request_sync_container",
)
REQUEST_ASYNC_CONTAINER: Final[ContextVar[AsyncContainer]] = ContextVar(
    "dishka_airflow_request_async_container",
)
