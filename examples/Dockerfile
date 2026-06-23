# Build context is the repository root (see docker-compose.yaml).
FROM apache/airflow:3.2.2

# hatch-vcs derives the version from git tags, which are not in the build
# context; pin a placeholder so the wheel builds without the .git directory.
ENV SETUPTOOLS_SCM_PRETEND_VERSION=0.1.0

# Install the integration package itself.
COPY --chown=airflow:root pyproject.toml README.md LICENSE /opt/dishka-airflow/
COPY --chown=airflow:root src /opt/dishka-airflow/src
RUN pip install --no-cache-dir /opt/dishka-airflow

# Install the layered example application (domain/application/infrastructure).
COPY --chown=airflow:root examples/pyproject.toml /opt/example-app/
COPY --chown=airflow:root examples/src /opt/example-app/src
RUN pip install --no-cache-dir /opt/example-app
