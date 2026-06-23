"""dishka providers wiring the layers together and integrating with Airflow."""

from airflow.sdk.types import TaskInstance
from dishka import Provider, Scope, provide

from myapp.application.interfaces import GreetingRepository
from myapp.application.services import GreetingService
from myapp.domain.entities import TaskReport
from myapp.infrastructure.persistence import LoggingGreetingRepository


class InfrastructureProvider(Provider):
    scope = Scope.APP

    @provide
    def greeting_repository(self) -> GreetingRepository:
        return LoggingGreetingRepository()


class ApplicationProvider(Provider):
    scope = Scope.APP

    @provide
    def greeting_service(self, repository: GreetingRepository) -> GreetingService:
        return GreetingService(repository=repository, prefix="Hello")


class AirflowIntegrationProvider(Provider):
    scope = Scope.REQUEST

    @provide
    def task_report(self, task_instance: TaskInstance) -> TaskReport:
        return TaskReport(
            task_id=task_instance.task_id,
            run_id=task_instance.run_id,
        )
