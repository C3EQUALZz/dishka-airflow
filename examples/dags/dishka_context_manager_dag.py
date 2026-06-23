"""Build a DAG with the context-manager style instead of the ``@dag`` decorator.

Tasks declared inside a ``with DAG(...)`` block attach to that DAG. Injection
works exactly the same: ``@task`` wraps the ``@inject``-decorated callable.
"""

import pendulum
from airflow.sdk import DAG, task
from dishka_airflow import FromDishka, inject

from myapp.application.services import GreetingService

with DAG(
    dag_id="dishka_context_manager",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dishka", "example"],
):

    @task
    @inject
    def greet(greeting_service: FromDishka[GreetingService]) -> str:
        greeting = greeting_service.greet("Airflow (context manager)")
        print(greeting.message)
        return greeting.message

    greet()
