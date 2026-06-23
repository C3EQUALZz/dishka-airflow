# AGENTS.md

Guidance for AI agents (and humans) working in this repository. Read it before
changing code. It records the design, the non-obvious technical constraints, the
typing/style rules the maintainer enforces, and the quality gate every change
must pass.

---

## 1. What this project is

`dishka-airflow` integrates the [dishka](https://github.com/reagento/dishka)
dependency-injection framework with **Apache Airflow 3.x (Task SDK)**.

**Design model — ag2-style, no global registration.** There is deliberately no
`setup_dishka(container)` global function. The container is passed explicitly to
a `DishkaPlugin` subclass. The reference implementation is
[`dishka-ag2`](https://github.com/C3EQUALZz/dishka-ag2) by the same maintainer —
match its conventions when in doubt.

Supported: Python ≥ 3.10, `dishka >= 1.10.1`, `apache-airflow-task-sdk >= 1.2.2`
(Airflow 3.2.x).

---

## 2. Source layout (`src/dishka_airflow/`)

The package is split into small single-purpose modules — **do not collapse them
back into one file**.

| Module | Responsibility |
| --- | --- |
| `_types.py` | `ParamsP` (ParamSpec) and `ReturnT` (TypeVar). |
| `_context.py` | Two `ContextVar`s: `REQUEST_SYNC_CONTAINER: ContextVar[Container]` and `REQUEST_ASYNC_CONTAINER: ContextVar[AsyncContainer]`. Split (not a union) so the resolution site stays precisely typed without casts. |
| `_provider.py` | `AirflowProvider(Provider)` — exposes `Context` and `TaskInstance` at `Scope.REQUEST` via `from_context`. |
| `_listener.py` | `_DishkaListener` (pluggy listener). Opens a `REQUEST` scope on task start, closes it on success/failure. |
| `_plugin.py` | `DishkaPlugin(AirflowPlugin)` — wires the listener in `__init_subclass__` when `container` is set. |
| `_injectors.py` | `inject` decorator (sync/async dispatch via `iscoroutinefunction`, `manage_scope=False`). |
| `__init__.py` | Re-exports `AirflowProvider`, `DishkaPlugin`, `FromDishka`, `inject`. |

Public API (the only names users import): `AirflowProvider`, `DishkaPlugin`,
`FromDishka`, `inject`.

---

## 3. How the wiring works (runtime flow)

1. The user subclasses `DishkaPlugin`, sets `name` and `container`, and drops it
   in Airflow's plugins folder. `__init_subclass__` builds a `_DishkaListener`
   from the container and stores it in `cls.listeners`.
2. Airflow's plugin loader reads `plugin.listeners` and registers the listener
   with the Task SDK's pluggy `ListenerManager`.
3. On `on_task_instance_running`, the listener calls
   `container(context={Context: ctx, TaskInstance: ti})` and stores the child
   container in the matching `ContextVar`.
4. Tasks decorated with `@inject` resolve `FromDishka[...]` parameters from that
   `ContextVar` (`manage_scope=False` — the listener owns the scope).
5. On `on_task_instance_success`/`failed`, the listener closes the request
   container.

---

## 4. Non-obvious technical constraints (read before touching internals)

These were all discovered the hard way. Changing them will break things.

- **`hookimpl` import:** `from airflow.listeners import hookimpl`. It is **not**
  in `airflow.sdk.listener`. Hookspecs live in
  `airflow.sdk._shared.listeners.spec.taskinstance`; hooks fire from
  `airflow/sdk/execution_time/task_runner.py` (`on_task_instance_running` in
  `_prepare`, success/failed in `finalize`).
- **Pluggy accepts a subset of hookspec args.** Our `_DishkaListener` hooks
  declare only the parameters they use (e.g. `on_task_instance_success(self)`
  with no args). This is intentional and keeps ruff's unused-argument rule
  happy. The caller must still pass all hookspec kwargs (`previous_state`,
  `task_instance`, and for `failed` also `error`).
- **`TaskInstance` import:** import from `airflow.sdk.types` (the public alias of
  `RuntimeTaskInstanceProtocol`, with `get_template_context()`/`task_id`). The
  `airflow.sdk.TaskInstance` re-export is **not visible to mypy** (dynamic). At
  runtime both are the same object. `Context` is fine from `airflow.sdk`.
- **Opening a scope needs no context-manager entry.** `Container.__enter__` and
  `AsyncContainer.__aenter__` just `return self`, so calling
  `container(context=...)` is enough to open the `REQUEST` scope.
- **Async teardown from a sync hook.** Listener hooks are synchronous, so
  `_close_async` spins up a throwaway `asyncio.new_event_loop()` and
  `run_until_complete(container.close())`. In production the hook fires outside
  any running loop, so this is safe. In tests (which run inside pytest's loop)
  you must drive the close via `asyncio.to_thread(...)` — see §6.
- **`inject` typing hack.** `inject` returns `Callable[..., ReturnT]` (and
  `Callable[..., Awaitable[ReturnT]]`), **not** `Callable[ParamsP, ReturnT]`.
  dishka strips the `FromDishka` parameters from the runtime signature, so the
  `...` parameter list is the honest type and lets callers invoke `task()`
  cleanly under mypy with no `# type: ignore` and no test helpers.
- **Decorator order with Airflow tasks:** `@task` outside, `@inject` inside.
  `inject` rewrites `__signature__` to drop `FromDishka` params, so Airflow sees
  a task with no inputs while dishka supplies them. Ordinary args/XCom values
  pass through untouched.

---

## 5. Typing & style rules (the maintainer enforces these strictly)

- **No `from __future__ import annotations`.** Python ≥ 3.10 evaluates `X | Y`
  unions natively; rely on that.
- **No `Any` in data/type positions.** `Any` is allowed only at the dishka
  boundary, exactly as `dishka-ag2` does it — e.g. `tuple[Any, ...]`,
  `dict[Any, Any]`, `inject(func: Callable[ParamsP, Any])`. Do **not** use
  `type` or `object` as stand-in types — the maintainer rejected those.
- **No `# type: ignore`.** It masks rather than explains. Express the real shape
  instead: the `Callable[..., R]` return hack, a `cast`, or an explicit
  boundary-`Any` alias for untyped third-party APIs (e.g. Airflow's
  `ListenerManager` is untyped → `_x: Any = ListenerManager`).
- **No inline comments in code.** Docstrings on modules/classes/functions are
  fine; `#` comments and `#:` attribute comments are not (tests may carry a
  short explanatory comment only when it documents a genuine subtlety).
- **Tests:** `make_*` factories live in `tests/factories.py`, **not** in
  `conftest.py`. Fixtures live in `conftest.py`. Prefer fixtures where possible.

---

## 6. Testing conventions

Structure mirrors `dishka-ag2`. 100% coverage is expected.

```
tests/
├── common.py        # AppProvider (NewType deps + Mock release tracking), FakeTaskInstance
├── conftest.py      # fixtures: app_provider, task_instance
├── factories.py     # make_task_instance (all make_* factories go here)
├── unit/
│   ├── conftest.py          # create_airflow_env(provider, *, use_async_container)
│   ├── test_errors.py
│   ├── inject/test_{sync,async}.py
│   ├── listener/app/test_{sync,async}.py
│   ├── listener/request/test_{sync,async}.py
│   └── plugin/test_plugin.py
└── integration/
    ├── conftest.py          # sync_env/async_env + build_listener_manager
    └── task/test_{sync,async}.py
```

Conventions and gotchas:

- **`test_sync.py` / `test_async.py` pairs** per feature, like ag2.
- **Explicit `@pytest.mark.asyncio()`** on every async test (strict asyncio
  mode; no `asyncio_mode = "auto"`). Config in `pyproject.toml`
  (`asyncio_default_fixture_loop_scope = "function"`, airflow deprecation filter).
- **Integration tests are pluggy-level, not full Airflow.** They build a real
  `airflow.sdk._shared.listeners.listener.ListenerManager`, `add_hookspecs`
  (`lifecycle`, `taskinstance`), `add_listener`, and dispatch via
  `manager.hook.on_task_instance_running(previous_state=..., task_instance=...)`.
  This validates that our `@hookimpl` signatures actually match Airflow's
  hookspecs — pluggy silently skips mismatched hooks otherwise. Spinning up a
  full Airflow runtime (scheduler/DB) is **not** done; it is the wrong cost for a
  DI library.
- **Async container close in tests:** the listener's sync close uses
  `run_until_complete`, which raises inside pytest's running loop. Drive it via
  `await asyncio.to_thread(listener.on_task_instance_success)` (a worker thread
  has no running loop).
- **ContextVar isolation is automatic** for async tests: each coroutine runs in
  a copied context, so `REQUEST_*_CONTAINER.set()` never leaks across tests.
  This is why the "no open scope → `LookupError`" test must be `async`.
- **mypy/Mock gotcha:** typeshed's `Mock` is seen as `Awaitable`, so `inject`'s
  async-first overload mis-picks for a `-> Mock` return. Avoid by appending
  inside the task and returning `None` (the ag2 pattern), not by returning the
  Mock.

---

## 7. The `examples/` deployment

A runnable Airflow 3.2.2 stack proving the integration end-to-end.

- **Layered, installable app** at `examples/src/myapp/` (src-layout, clean
  architecture): `domain/` (pure value objects), `application/` (Protocol ports +
  use-case services), `infrastructure/` (adapters + dishka providers + the
  Airflow integration provider). `dags/` and `plugins/` are thin,
  path-discovered adapters that import `myapp`.
- **Four DAGs**, one per case: `dishka_simple` (`@dag` decorator),
  `dishka_context_manager` (`with DAG(...)` style), `dishka_with_params`
  (`FromDishka` alongside XCom task args — proves data is not clobbered),
  `dishka_airflow_provider` (Context + TaskInstance injection).
- **Docker:** `docker compose up --build` from `examples/`. Postgres +
  `airflow standalone` (LocalExecutor). The build context is the **repo root**
  (the Dockerfile installs the integration package), so the ignore file is
  `examples/Dockerfile.dockerignore` (BuildKit per-Dockerfile ignore; paths are
  relative to the context root).
- **Example tooling** (separate files, mirroring this repo's style):
  `examples/ruff.toml`, `examples/mypy.ini`, and `examples/pyproject.toml` with a
  `[tool.importlinter]` layers contract (`domain < application < infrastructure`).
  Run from `examples/`: `uv run ruff check`, `uv run mypy`, `uv run lint-imports`.

Example-specific gotchas:

- A manually-triggered run without a logical date has **no `ds` context key** —
  use always-present keys like `context["dag"].dag_id`.
- hatchling fails if `pyproject` `readme = ...` points at a file not copied into
  the image — omit `readme` from the example package metadata.
- `myapp` must be pip-installed (or on `PYTHONPATH`) so plugins **and** DAGs can
  import it.
- To run example tooling against the main `.venv`, use
  `uv run --no-project ...` (avoids uv trying to resolve the unpublished example
  project); `lint-imports` needs `PYTHONPATH=src`.

---

## 8. Quality gate — run before declaring done

The maintainer requires that **tests are type-checked and linted at full
strictness too** (mypy `files = src, tests`, ruff `select = ALL`).

From the repo root (uses the project `justfile`):

```bash
just ruff-format ruff-check    # ruff format + lint (select = ALL, src + tests)
just mypy                      # strict mypy on src AND tests
just _codespell                # spell check
just bandit                    # security lint
uv run --active pytest --cov=dishka_airflow --cov-report=term-missing   # 100% coverage
```

For the example app, from `examples/`:

```bash
uv run --no-project ruff check
uv run --no-project mypy
PYTHONPATH=src uv run --no-project lint-imports
```

Everything must be green. The example DAGs were verified to run successfully in
real Airflow 3.2.2 — if you change `examples/`, re-verify with
`docker compose up --build`.

---

## 9. Issue tracker & triage (GitHub via `gh`)

Issues and PRDs live as GitHub issues in `C3EQUALZz/dishka-airflow`. Use the
`gh` CLI; it infers the repo from `git remote -v` inside a clone.

- Create: `gh issue create --title "..." --body "..."` (heredoc for multi-line).
- Read: `gh issue view <number> --comments`.
- List: `gh issue list --state open --json number,title,body,labels,comments`.
- Comment / label / close: `gh issue comment`, `gh issue edit --add-label` /
  `--remove-label`, `gh issue close --comment`.

**External PRs are a triage surface** (treated as feature requests). Use the
`gh pr ...` equivalents; GitHub shares one number space across issues and PRs, so
resolve a bare `#N` with `gh pr view N`, falling back to `gh issue view N`.

**Triage labels** — five canonical roles → repo labels:

| Role | Label | Meaning |
| --- | --- | --- |
| needs-triage | `needs-triage` | Maintainer needs to evaluate |
| needs-info | `question` | Waiting on reporter |
| ready-for-agent | `ready-for-agent` | Fully specified, ready for an AFK agent |
| ready-for-human | `help wanted` | Requires human implementation |
| wontfix | `wontfix` | Will not be actioned |

`needs-triage` and `ready-for-agent` may not exist yet — create on first use
with `gh label create <name> --description "..."`.

---

## 10. Commit / PR etiquette

- Branch off `main`; never commit or push unless explicitly asked.
- Keep diffs minimal and in the surrounding style (match comment density, naming,
  idiom). Do not introduce inline comments (§5).
- Run the full quality gate (§8) before reporting work complete.
