# dishka-airflow example

A runnable Apache Airflow 3 deployment that wires
[dishka](https://github.com/reagento/dishka) dependency injection into task
execution via `DishkaPlugin`. The business logic lives in a layered, installable
`myapp` package (clean architecture); the DAGs and plugin are thin adapters.

## Layout

```
examples/
├── docker-compose.yaml          # Postgres + Airflow (standalone, LocalExecutor)
├── Dockerfile                   # apache/airflow:3.2.2 + dishka-airflow + myapp
├── pyproject.toml               # installable `myapp` package + tooling + layer contracts
├── ruff.toml                    # standalone lint config
├── mypy.ini                     # standalone strict type-check config
├── src/myapp/                   # the application (src-layout)
│   ├── domain/                  #   entities / value objects (no framework deps)
│   ├── application/             #   ports (interfaces) + use cases (services)
│   └── infrastructure/          #   adapters + dishka providers + Airflow integration
├── plugins/
│   └── dishka_plugin.py         # DishkaPlugin holding the assembled container
└── dags/
    ├── dishka_simple_dag.py             # @dag decorator + APP-scoped service
    ├── dishka_context_manager_dag.py    # `with DAG(...)` context-manager style
    ├── dishka_with_params_dag.py        # FromDishka alongside XCom task arguments
    └── dishka_airflow_provider_dag.py   # inject Context + a REQUEST-scoped value
```

## How it works

1. `plugins/dishka_plugin.py` assembles a container from the layered `myapp`
   providers plus `AirflowProvider`, and assigns it to a `DishkaPlugin` subclass.
   Airflow loads the plugin and registers its listener.
2. On every task instance run the listener opens a dishka `REQUEST` scope and
   closes it on success/failure.
3. Tasks declare dependencies with `FromDishka[...]` and are wrapped with
   `@inject`. `AirflowProvider` exposes `Context` and `TaskInstance` at `REQUEST`
   scope, so they inject like any other dependency.

Decorator order is `@task` outside, `@inject` inside: `inject` strips only the
`FromDishka` parameters from the signature, so ordinary Airflow arguments (such
as XCom values) still arrive untouched.

## Run

```bash
cd examples
docker compose up --build
```

`airflow standalone` migrates the database, creates an admin user and starts
every component. Grab the generated admin password from the logs:

```bash
docker compose logs airflow | grep -i password
```

Open <http://localhost:8080>, log in as `admin`, then trigger the DAGs and read
the task logs to see the injected values:

- **`dishka_simple`** → `Hello, Airflow!`
- **`dishka_context_manager`** → `Hello, Airflow (context manager)!`
- **`dishka_with_params`** → `Hello, Alice!` (the `name` arrives via XCom; the
  service is injected)
- **`dishka_airflow_provider`** → `task=... run=... dag=...`

Editing `dags/` or `plugins/` takes effect live (they are mounted). Editing the
`myapp` package requires a rebuild (`docker compose up --build`).

## Tear down

```bash
docker compose down -v
```

## Quality checks

The example carries its own tooling, run from this directory:

```bash
uv run ruff check          # lint dags, plugins and the package
uv run mypy                # strict type-check the myapp package
uv run lint-imports        # enforce the domain → application → infrastructure layering
```
