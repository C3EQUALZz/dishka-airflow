# Airflow integration for Dishka

[![Downloads](https://static.pepy.tech/personalized-badge/dishka-airflow?period=month&units=international_system&left_color=grey&right_color=green&left_text=downloads/month)](https://www.pepy.tech/projects/dishka-airflow)
[![Package version](https://img.shields.io/pypi/v/dishka-airflow?label=PyPI)](https://pypi.org/project/dishka-airflow)
[![Supported Python versions](https://img.shields.io/pypi/pyversions/dishka-airflow.svg)](https://pypi.org/project/dishka-airflow)

Though it is not required, you can use *dishka-airflow* integration. It features:

* `REQUEST` scope management tied to the Airflow task instance lifecycle via `DishkaPlugin` and `@inject`
* `AirflowProvider` for accessing `Context` and `TaskInstance` in the container at `REQUEST` scope
* Automatic dependency resolution for `@task`, `@setup`, `@teardown`, `@asset` and `@asset.multi` decorated functions
* Both synchronous and asynchronous containers and task functions are supported

### Scope mapping

| Scope     | Airflow lifecycle                                                                   | Description                                                    |
|-----------|-------------------------------------------------------------------------------------|----------------------------------------------------------------|
| `APP`     | Root container (lives for the process lifetime)                                     | Shared across all task runs.                                   |
| `REQUEST` | `on_task_instance_running` → `on_task_instance_success` / `on_task_instance_failed` | Opened once per task execution. Closed when the task finishes. |

## Supported Airflow features

| Airflow decorator      | `Scope.APP` | `Scope.REQUEST` | Notes                                                                                    |
|------------------------|-------------|-----------------|------------------------------------------------------------------------------------------|
| `@task @inject`        | yes         | yes             | Main supported path. Decorator order: `@task` outside, `@inject` inside.                 |
| `@setup @inject`       | yes         | yes             | Setup tasks run before other tasks in their DAG/TaskGroup context.                       |
| `@teardown @inject`    | yes         | yes             | Teardown tasks get a fresh `REQUEST` scope independent of the main task's scope.         |
| `@asset @inject`       | yes         | yes             | Module-level definition required (`@asset` rejects nested functions).                    |
| `@asset.multi @inject` | yes         | yes             | Same restriction as `@asset`. Decorator order: `@asset.multi` outside, `@inject` inside. |

See the examples directory for a runnable deployment:

* `examples/dags/dishka_simple_dag.py` — `@task @inject` with an APP-scoped service.
* `examples/dags/dishka_context_manager_dag.py` — `with DAG(...)` context-manager style.
* `examples/dags/dishka_with_params_dag.py` — XCom task arguments alongside injected dependencies.
* `examples/dags/dishka_airflow_provider_dag.py` — inject `Context` and a REQUEST-scoped value.

## Installation

Install using `pip`

```sh
pip install dishka-airflow
```

Or with `uv`

```sh
uv add dishka-airflow
```

## How to use

1. Import

```python
from dishka_airflow import AirflowProvider, DishkaPlugin, FromDishka, inject
from dishka import make_container, Provider, provide, Scope
```

2. Create a provider. Use `Scope.APP` for long-lived singletons (database clients,
repositories, configuration). Use `Scope.REQUEST` for per-task objects. Add
`AirflowProvider` to expose `Context` and `TaskInstance`.

```python
from airflow.sdk import Context
from airflow.sdk.types import TaskInstance

class MyProvider(Provider):
    @provide(scope=Scope.APP)
    def greeting_service(self) -> GreetingService:
        return GreetingService()

    @provide(scope=Scope.REQUEST)
    def task_report(self, ti: TaskInstance) -> TaskReport:
        return TaskReport(task_id=ti.task_id, run_id=ti.run_id)
```

3. Mark the task parameters that should be injected with `FromDishka[]`

```python
from airflow.sdk import task
from dishka_airflow import FromDishka, inject

@task
@inject
def greet(
    name: str,
    service: FromDishka[GreetingService],
) -> str:
    return service.greet(name)
```

4. Create a `DishkaPlugin` subclass in your Airflow `plugins/` directory and
assign a container. Airflow discovers it automatically and registers the listener
that manages the `REQUEST` scope for every task run.

```python
# plugins/dishka_plugin.py
from dishka import make_container
from dishka_airflow import AirflowProvider, DishkaPlugin

from myapp.providers import MyProvider


class MyDishkaPlugin(DishkaPlugin):
    name = "my_dishka_plugin"
    container = make_container(MyProvider(), AirflowProvider())
```

That is all — no further wiring is needed. Once Airflow loads the plugin,
every `@inject`-decorated task function will have its `FromDishka` dependencies
resolved automatically.

## Decorator ordering

Decorator order matters. `@inject` must be the **innermost** decorator so it
wraps the raw function and strips `FromDishka` parameters from the signature
before Airflow inspects it. Airflow decorators (`@task`, `@setup`, `@teardown`,
`@asset`, `@asset.multi`) must be **outermost**.

```python
# Correct
@task
@inject
def my_task(dep: FromDishka[MyService]) -> str: ...

# Wrong — Airflow sees the unstripped FromDishka parameter
@inject
@task
def my_task(dep: FromDishka[MyService]) -> str: ...
```

### XCom arguments

`@inject` only removes `FromDishka` parameters. Ordinary Airflow arguments
(XCom values, operator links) are preserved in the signature and passed through
as usual.

```python
@task
@inject
def consume(
    name: str,                              # XCom value from upstream task
    service: FromDishka[GreetingService],   # injected by dishka
) -> str:
    return service.greet(name)
```

### Assets

`@asset` and `@asset.multi` require module-level function definitions. Defining
an asset inside a test or another function raises a `ValueError` at decoration
time.

```python
# Module level — required for @asset
@asset(schedule=None)
@inject
def my_asset(service: FromDishka[MyService]) -> str:
    return service.run()
```

## AirflowProvider integration types

`AirflowProvider` registers the following Airflow objects as context
dependencies so you can use them as factory parameters in your providers.

| Type            | Scope     | Description                                          |
|-----------------|-----------|------------------------------------------------------|
| `Context`       | `REQUEST` | Airflow template context for the current task run    |
| `TaskInstance`  | `REQUEST` | The running task instance object                     |

```python
from airflow.sdk import Context
from airflow.sdk.types import TaskInstance
from dishka import Provider, Scope, provide

class ReportProvider(Provider):
    @provide(scope=Scope.REQUEST)
    def task_report(self, ti: TaskInstance, ctx: Context) -> TaskReport:
        return TaskReport(
            task_id=ti.task_id,
            run_id=ti.run_id,
            dag_id=ctx["dag"].dag_id,
        )
```

## Async containers

Pass an `AsyncContainer` to `DishkaPlugin` when you use async providers (e.g.
`AsyncEngine`, async HTTP clients). `@inject` detects the container kind
automatically and dispatches to the right resolution path.

An `atexit` handler is registered to close the async container when the task
subprocess exits.

```python
from dishka import make_async_container
from dishka_airflow import AirflowProvider, DishkaPlugin

from myapp.providers import AsyncMyProvider


class MyDishkaPlugin(DishkaPlugin):
    name = "my_dishka_plugin"
    container = make_async_container(AsyncMyProvider(), AirflowProvider())
```

## Full example

```python
# plugins/dishka_plugin.py
from dishka import Provider, Scope, make_container, provide
from dishka_airflow import AirflowProvider, DishkaPlugin


class GreetingService:
    def greet(self, name: str) -> str:
        return f"Hello, {name}!"


class MyProvider(Provider):
    @provide(scope=Scope.APP)
    def greeting_service(self) -> GreetingService:
        return GreetingService()


class MyDishkaPlugin(DishkaPlugin):
    name = "my_dishka_plugin"
    container = make_container(MyProvider(), AirflowProvider())
```

```python
# dags/my_dag.py
import pendulum
from airflow.sdk import dag, task
from dishka_airflow import FromDishka, inject

from myapp.services import GreetingService


@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
)
def my_dag() -> None:
    @task
    @inject
    def greet(service: FromDishka[GreetingService]) -> str:
        message = service.greet("Airflow")
        print(message)
        return message

    greet()


my_dag()
```

## Run the example

A fully wired Docker deployment lives in the `examples/` directory. It bundles
a layered `myapp` package (domain / application / infrastructure), four
example DAGs and a `DishkaPlugin`.

```sh
cd examples
docker compose up --build
```

Open <http://localhost:8080>, log in as `admin` (password in the logs), and
trigger the example DAGs to see injected values in the task logs.

```sh
docker compose down -v   # tear down and remove volumes
```
