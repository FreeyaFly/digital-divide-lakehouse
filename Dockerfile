# Task image: the World Bank extractor and the dbt project. Airflow runs it with DockerOperator
# (docs/adr/0006-orchestration.md). No credentials are baked in; they are passed at run time.
#
#   docker build -t ddlake-tasks .
#   docker run --rm ddlake-tasks dbt --version
#   docker run --rm -e DBT_DATABRICKS_HOST -e DBT_DATABRICKS_HTTP_PATH -e DBT_DATABRICKS_TOKEN \
#       ddlake-tasks dbt build
#   docker run --rm -e DDLAKE_STORAGE_ACCOUNT -e AZURE_TENANT_ID -e AZURE_CLIENT_ID \
#       -e AZURE_CLIENT_SECRET ddlake-tasks python -m ingestion.worldbank --target adls

# Same Python minor version as Databricks serverless; tag and digest pinned for repeatable builds.
ARG PYTHON_IMAGE=python:3.12.15-slim@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1

# --- Build stage: install the locked dependencies into a virtual environment ----------------
FROM ${PYTHON_IMAGE} AS build
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
RUN python -m venv /opt/venv
ENV PATH=/opt/venv/bin:$PATH
COPY requirements-tasks.txt /tmp/
# Hashes must match the lock file; wheels only, so nothing is compiled. One exception:
# dbt-core-experimental-parser is published only as an sdist whose build step downloads the
# prebuilt wheel for this platform from the dbt release and checks its sha256.
RUN pip install --require-hashes --only-binary=:all: \
        --no-binary=dbt-core-experimental-parser -r /tmp/requirements-tasks.txt

# --- Runtime stage: the environment and the code, no build tools or caches -------------------
FROM ${PYTHON_IMAGE}
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONPATH=/app \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/app/transform/dbt \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false
RUN useradd --create-home --uid 10001 app
COPY --from=build /opt/venv /opt/venv
COPY --chown=app:app ingestion /app/ingestion
# dbt writes target/ and logs/ into the project folder, so the app user owns it.
COPY --chown=app:app transform/dbt /app/transform/dbt
USER app
WORKDIR /app/transform/dbt
CMD ["dbt", "--version"]
