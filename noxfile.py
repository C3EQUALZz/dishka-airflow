import nox

nox.options.default_venv_backend = "uv"
nox.options.reuse_existing_virtualenvs = True
nox.options.stop_on_first_error = True


PYTHON_VERSIONS = ["3.10", "3.11", "3.12", "3.13", "3.14"]
DISHKA_VERSIONS = [
    nox.param("1.10.1", id="dishka-1.10.1"),
    nox.param(None, id="dishka-latest-compatible"),
]
AIRFLOW_SDK_VERSIONS = [
    nox.param("1.2.2", id="airflow-sdk-1.2.2"),
    nox.param(None, id="airflow-sdk-latest-compatible"),
]


def load_pyproject() -> dict:
    """Load project configuration."""
    return nox.project.load_toml("pyproject.toml")


def load_project_dependency(dependency_name: str) -> str:
    """Load dependency specifier from pyproject.toml."""
    toml_data = load_pyproject()
    dependencies = toml_data["project"]["dependencies"]
    for dependency in dependencies:
        if dependency.startswith(dependency_name):
            return dependency
    msg = f"Dependency {dependency_name!r} not found in pyproject.toml"
    raise LookupError(msg)


def dependency_spec(dependency: str, version: str | None = None) -> str:
    """Return pinned dependency or pyproject-compatible latest spec."""
    return f"{dependency}=={version}" if version else load_project_dependency(dependency)


def load_test_dependencies() -> list[str]:
    """Load test dependencies from pyproject.toml."""
    toml_data = load_pyproject()
    return toml_data["dependency-groups"]["test"]


@nox.session(python=PYTHON_VERSIONS)
@nox.parametrize("airflow_sdk_version", AIRFLOW_SDK_VERSIONS)
@nox.parametrize("dishka_version", DISHKA_VERSIONS)
def tests(
    session: nox.Session,
    dishka_version: str | None,
    airflow_sdk_version: str | None,
) -> None:
    """Run tests across Python, dishka, and apache-airflow-task-sdk versions."""
    session.install(*load_test_dependencies())
    session.install(
        dependency_spec("dishka", dishka_version),
        dependency_spec("apache-airflow-task-sdk", airflow_sdk_version),
    )
    session.install("-e", ".", "--no-deps")

    pytest_args = [
        "tests",
        "--cov=dishka_airflow",
        "--cov-report=term-missing",
        "--cov-append",
        "--cov-config=.coveragerc",
        *session.posargs,
    ]
    session.run(
        "pytest",
        *pytest_args,
        env={
            "COVERAGE_FILE": f".coverage.{session.name}",
        },
    )


@nox.session
def coverage(session: nox.Session) -> None:
    """Combine per-session coverage and enforce the threshold."""
    session.install("coverage")
    session.run("coverage", "combine")
    session.run("coverage", "report", "--fail-under=100")
