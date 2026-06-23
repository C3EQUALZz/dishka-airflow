"""dishka-airflow: dependency injection for Apache Airflow 3.x via dishka.

The integration follows the ``dishka-ag2`` style: there is no global
``setup_dishka`` function. The container is passed explicitly to a
:class:`DishkaPlugin` subclass, which registers a listener that opens a
``REQUEST`` scope for every task instance run and closes it when the task
finishes. Inside a task, dependencies are resolved with :func:`inject`.
"""

from dishka import FromDishka

from dishka_airflow._injectors import inject
from dishka_airflow._plugin import DishkaPlugin
from dishka_airflow._provider import AirflowProvider

__all__ = [
    "AirflowProvider",
    "DishkaPlugin",
    "FromDishka",
    "inject",
]
