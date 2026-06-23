"""Register a dishka container with Airflow via DishkaPlugin.

Airflow discovers this module in the plugins folder, reads the ``listeners``
attribute that :class:`DishkaPlugin` populates, and wires the dishka REQUEST
scope into every task instance run. The container is assembled from the
layered ``myapp`` providers plus the integration's own ``AirflowProvider``.
"""

from dishka import make_container
from dishka_airflow import AirflowProvider, DishkaPlugin

from myapp.infrastructure.providers import (
    AirflowIntegrationProvider,
    ApplicationProvider,
    InfrastructureProvider,
)


class ExampleDishkaPlugin(DishkaPlugin):
    name = "example_dishka_plugin"
    container = make_container(
        InfrastructureProvider(),
        ApplicationProvider(),
        AirflowIntegrationProvider(),
        AirflowProvider(),
    )
