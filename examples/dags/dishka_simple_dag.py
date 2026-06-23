"""Inject an APP-scoped application service into a TaskFlow task.

Decorator order matters: ``@task`` wraps the already-injected callable, so
Airflow sees a task with no inputs while dishka supplies the ``GreetingService``
(and its repository dependency) at execution time.
"""

import pendulum
from airflow.sdk import dag, task
from dishka_airflow import FromDishka, inject

from myapp.application.services import GreetingService


@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dishka", "example"],
)
def dishka_simple() -> None:
    @task
    @inject
    def greet(greeting_service: FromDishka[GreetingService]) -> str:
        greeting = greeting_service.greet("Airflow")
        print(greeting.message)
        return greeting.message

    greet()


dishka_simple()
