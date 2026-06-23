"""dishka provider exposing Airflow execution objects."""

from airflow.sdk import Context
from airflow.sdk.types import TaskInstance
from dishka import Provider, Scope, from_context


class AirflowProvider(Provider):
    """Provides the Airflow execution objects at ``REQUEST`` scope.

    Add it to your container to inject the task template :class:`Context`
    and the running :class:`TaskInstance` into task functions.
    """

    context = from_context(provides=Context, scope=Scope.REQUEST)
    task_instance = from_context(provides=TaskInstance, scope=Scope.REQUEST)
