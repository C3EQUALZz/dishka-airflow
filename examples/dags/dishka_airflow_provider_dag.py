"""Inject the Airflow Context and a REQUEST-scoped value built from it.

``AirflowProvider`` exposes ``Context`` and ``TaskInstance`` at REQUEST scope;
``AirflowIntegrationProvider`` consumes ``TaskInstance`` to build a domain
``TaskReport``.
"""

import pendulum
from airflow.sdk import Context, dag, task
from dishka_airflow import FromDishka, inject

from myapp.domain.entities import TaskReport


@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dishka", "example"],
)
def dishka_airflow_provider() -> None:
    @task
    @inject
    def report(
        context: FromDishka[Context],
        task_report: FromDishka[TaskReport],
    ) -> str:
        summary = f"{task_report.describe()} dag={context['dag'].dag_id}"
        print(summary)
        return summary

    report()


dishka_airflow_provider()
