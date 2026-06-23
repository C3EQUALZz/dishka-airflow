"""Show that ``FromDishka`` does not interfere with task data (XCom args).

The ``consume`` task receives ``name`` from the upstream ``produce`` task via
XCom and also gets ``GreetingService`` injected. ``inject`` only removes the
``FromDishka`` parameters, so ordinary Airflow arguments still arrive intact.
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
def dishka_with_params() -> None:
    @task
    def produce() -> str:
        return "Alice"

    @task
    @inject
    def consume(name: str, greeting_service: FromDishka[GreetingService]) -> str:
        greeting = greeting_service.greet(name)
        print(greeting.message)
        return greeting.message

    consume(produce())


dishka_with_params()
