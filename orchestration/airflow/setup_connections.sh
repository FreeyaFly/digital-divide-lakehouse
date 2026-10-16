#!/usr/bin/env bash
# Creates (or replaces) the Airflow connections and variables that the ddlake_refresh DAG needs.
#
# Non-secret values come from the environment. Secrets are taken from the environment when set,
# otherwise prompted for without echo; they are never printed or written to a file, only stored
# in the Airflow metadata database (encrypted with Airflow's Fernet key). The script prints each
# secret's length and, for Databricks tokens, the first four characters, as a paste check.
#
# Required environment:
#   AZURE_TENANT_ID, AZURE_CLIENT_ID        pipeline service principal (terraform output)
#   DDLAKE_STORAGE_ACCOUNT                  ADLS account (terraform output storage_account_name)
#   DDLAKE_REFRESH_JOB_ID                   Databricks job id of the bundle job ddlake_refresh
#   DBT_DATABRICKS_HOST, DBT_DATABRICKS_HTTP_PATH
# Secrets (prompted for if not set):
#   AZURE_CLIENT_SECRET, AIRFLOW_DATABRICKS_TOKEN, DBT_DATABRICKS_TOKEN
set -euo pipefail

export AIRFLOW_HOME="${AIRFLOW_HOME:-$HOME/airflow}"
AIRFLOW="${AIRFLOW_BIN:-$HOME/venvs/airflow/bin/airflow}"

for name in AZURE_TENANT_ID AZURE_CLIENT_ID DDLAKE_STORAGE_ACCOUNT DDLAKE_REFRESH_JOB_ID \
            DBT_DATABRICKS_HOST DBT_DATABRICKS_HTTP_PATH; do
  if [ -z "${!name:-}" ]; then
    echo "missing environment variable: $name" >&2
    exit 1
  fi
done

ask_secret() {
  local name=$1 prompt=$2
  if [ -z "${!name:-}" ]; then
    read -rsp "$prompt (input hidden): " "${name?}"
    echo
  fi
  if [ -z "${!name}" ]; then
    echo "$name is empty" >&2
    exit 1
  fi
}

ask_secret AZURE_CLIENT_SECRET "Azure client secret of the pipeline service principal"
ask_secret AIRFLOW_DATABRICKS_TOKEN "Databricks token for Airflow"
ask_secret DBT_DATABRICKS_TOKEN "Databricks token for dbt"

echo "AZURE_CLIENT_SECRET length ${#AZURE_CLIENT_SECRET}"
echo "AIRFLOW_DATABRICKS_TOKEN length ${#AIRFLOW_DATABRICKS_TOKEN}, starts ${AIRFLOW_DATABRICKS_TOKEN:0:4}"
echo "DBT_DATABRICKS_TOKEN length ${#DBT_DATABRICKS_TOKEN}, starts ${DBT_DATABRICKS_TOKEN:0:4}"

host="${DBT_DATABRICKS_HOST#https://}"

replace_connection() {
  local id=$1
  shift
  "$AIRFLOW" connections delete "$id" >/dev/null 2>&1 || true
  "$AIRFLOW" connections add "$id" "$@" >/dev/null
  echo "connection $id: set"
}

replace_connection databricks_default --conn-type databricks \
  --conn-host "https://$host" --conn-password "$AIRFLOW_DATABRICKS_TOKEN"
replace_connection azure_pipeline --conn-type generic \
  --conn-login "$AZURE_CLIENT_ID" --conn-password "$AZURE_CLIENT_SECRET" \
  --conn-extra "{\"tenant_id\": \"$AZURE_TENANT_ID\"}"
replace_connection dbt_databricks --conn-type generic \
  --conn-host "$host" --conn-password "$DBT_DATABRICKS_TOKEN" \
  --conn-extra "{\"http_path\": \"$DBT_DATABRICKS_HTTP_PATH\"}"

"$AIRFLOW" variables set ddlake_storage_account "$DDLAKE_STORAGE_ACCOUNT" >/dev/null
"$AIRFLOW" variables set ddlake_refresh_job_id "$DDLAKE_REFRESH_JOB_ID" >/dev/null
echo "variables ddlake_storage_account, ddlake_refresh_job_id: set"
