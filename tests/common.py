from collections.abc import Iterable
from typing import NewType, cast
from unittest.mock import Mock

from airflow.sdk import Context
from dishka import Provider, Scope, provide

AppDep = NewType("AppDep", str)
APP_DEP_VALUE = AppDep("APP")

RequestDep = NewType("RequestDep", str)
REQUEST_DEP_VALUE = RequestDep("REQUEST")

AppMock = NewType("AppMock", Mock)


class AppProvider(Provider):
    def __init__(self) -> None:
        super().__init__()
        self.app_released = Mock()
        self.request_released = Mock()
        self.mock = Mock()
        self._app_mock = AppMock(Mock())

    @provide(scope=Scope.APP)
    def app(self) -> Iterable[AppDep]:
        yield APP_DEP_VALUE
        self.app_released()

    @provide(scope=Scope.APP)
    def app_mock(self) -> AppMock:
        return self._app_mock

    @provide(scope=Scope.REQUEST)
    def request(self) -> Iterable[RequestDep]:
        yield REQUEST_DEP_VALUE
        self.request_released()

    @provide(scope=Scope.REQUEST)
    def get_mock(self) -> Mock:
        return self.mock


class FakeTaskInstance:
    """Minimal ``RuntimeTaskInstance`` stand-in for listener tests."""

    def __init__(self, task_id: str = "demo_task") -> None:
        self.task_id = task_id

    def get_template_context(self) -> Context:
        return cast(
            "Context",
            {"ds": "2026-06-22", "run_id": "run_1", "task_id": self.task_id},
        )
