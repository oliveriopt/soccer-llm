
Aquí va una versión más seria para Composer, con Variable, labels, timeout, y lista para correr diario o manual. La idea es que el lookback_days salga de Airflow y no quede quemado en el código.

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


PROJECT_ID = "rxo-dataeng-datalake-uat"
DATASET_ID = "sqlserver_to_bq_silver"
PROCEDURE_NAME = "sp_load_orders"
BQ_LOCATION = "US"   # cámbialo si tu dataset está en otra región

DEFAULT_LOOKBACK_DAYS = int(Variable.get("sp_load_orders_lookback_days", default_var=1))

default_args = {
    "owner": "dataeng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


with DAG(
    dag_id="dag_sp_load_orders_daily",
    description="Ejecuta diariamente la SP de Orders en BigQuery",
    default_args=default_args,
    start_date=datetime(2026, 3, 10),
    schedule="0 3 * * *",   # todos los días a las 03:00
    catchup=False,
    max_active_runs=1,
    tags=["bq", "silver", "orders", "sp"],
) as dag:

    run_sp_load_orders = BigQueryInsertJobOperator(
        task_id="run_sp_load_orders",
        configuration={
            "query": {
                "query": f"""
DECLARE v_lookback_days INT64 DEFAULT {DEFAULT_LOOKBACK_DAYS};

CALL `{PROJECT_ID}.{DATASET_ID}.{PROCEDURE_NAME}`(v_lookback_days);
                """,
                "useLegacySql": False,
            },
            "labels": {
                "domain": "orders",
                "layer": "silver",
                "orchestrator": "airflow",
            },
        },
        location=BQ_LOCATION,
        gcp_conn_id="google_cloud_default",
        execution_timeout=timedelta(hours=2),
    )

    run_sp_load_orders

Variable que debes crear en Airflow

Crea esta variable en Composer / Airflow:

sp_load_orders_lookback_days = 1

Así después cambias el lookback sin tocar el DAG.

⸻

Qué ejecuta realmente

Este DAG termina ejecutando esto:

DECLARE v_lookback_days INT64 DEFAULT 1;
CALL `rxo-dataeng-datalake-uat.sqlserver_to_bq_silver.sp_load_orders`(v_lookback_days);


⸻

Si quieres correrlo manual con otro valor

La forma más limpia es una versión con dag_run.conf, para que cuando lo lances manualmente desde Airflow UI puedas pasar:

{
  "lookback_days": 7
}

Te dejo esa versión también:

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


PROJECT_ID = "rxo-dataeng-datalake-uat"
DATASET_ID = "sqlserver_to_bq_silver"
PROCEDURE_NAME = "sp_load_orders"
BQ_LOCATION = "US"

DEFAULT_LOOKBACK_DAYS = int(Variable.get("sp_load_orders_lookback_days", default_var=1))

default_args = {
    "owner": "dataeng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


with DAG(
    dag_id="dag_sp_load_orders_daily",
    description="Ejecuta diariamente la SP de Orders en BigQuery",
    default_args=default_args,
    start_date=datetime(2026, 3, 10),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["bq", "silver", "orders", "sp"],
) as dag:

    run_sp_load_orders = BigQueryInsertJobOperator(
        task_id="run_sp_load_orders",
        configuration={
            "query": {
                "query": f"""
DECLARE v_lookback_days INT64 DEFAULT COALESCE(
  CAST({{{{ dag_run.conf.get('lookback_days', None) }}}} AS INT64),
  {DEFAULT_LOOKBACK_DAYS}
);

CALL `{PROJECT_ID}.{DATASET_ID}.{PROCEDURE_NAME}`(v_lookback_days);
                """,
                "useLegacySql": False,
            },
            "labels": {
                "domain": "orders",
                "layer": "silver",
                "orchestrator": "airflow",
            },
        },
        location=BQ_LOCATION,
        gcp_conn_id="google_cloud_default",
        execution_timeout=timedelta(hours=2),
    )

    run_sp_load_orders

Ejemplo de ejecución manual

En Airflow UI, Trigger DAG y en JSON pones:

{
  "lookback_days": 7
}

Si no mandas nada, usa la variable:

sp_load_orders_lookback_days


⸻

Recomendación práctica

Para producción, yo usaría esta convención:
	•	diario: lookback_days = 1
	•	rerun manual corto: 7
	•	backfill operativo: 30 o 90

⸻

Nombre de archivo sugerido

dag_sp_load_orders_daily.py

Carpeta

En Composer:

/dags/dag_sp_load_orders_daily.py

Si quieres, te doy ahora una tercera versión con TaskGroup, task de validación previa y task final de auditoría/log.