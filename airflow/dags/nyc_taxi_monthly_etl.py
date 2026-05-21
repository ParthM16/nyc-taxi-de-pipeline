"""
airflow/dags/nyc_taxi_monthly_etl.py
=====================================
Airflow DAG: orchestrates the NYC Taxi PySpark ETL on the 2nd of each month.
Runs for the prior month's data automatically.

Stack used here is intentionally simple for Stage 1.
In Stage 2 (Snowflake + dbt project) this pattern gets extended with
dbt transformation tasks and Snowflake load operators.

Setup:
    pip install apache-airflow apache-airflow-providers-google
    Set Airflow Variables: gcp_project_id, gcp_bucket, dataproc_region, dataproc_cluster
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator

# ── Config from Airflow Variables (set in UI or CLI) ─────────────────────────
PROJECT_ID = Variable.get("gcp_project_id", default_var="your-project-id")
BUCKET = Variable.get("gcp_bucket", default_var="nyc-taxi-de-your-project")
REGION = Variable.get("dataproc_region", default_var="us-central1")
CLUSTER = Variable.get("dataproc_cluster", default_var="nyc-taxi-cluster")

PYSPARK_URI = f"gs://{BUCKET}/pyspark_jobs/etl_pipeline.py"
BQ_JAR = "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"

DEFAULT_ARGS = {
    "owner": "parth",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "start_date": datetime(2024, 1, 1),
}


def compute_partition(**context) -> dict:
    """Compute year/month for the previous month from execution date."""
    exec_date = context["execution_date"]
    first_of_month = exec_date.replace(day=1)
    last_month = first_of_month - timedelta(days=1)
    return {
        "year": last_month.strftime("%Y"),
        "month": last_month.strftime("%m"),
    }


def log_run_info(**context) -> None:
    partition = context["ti"].xcom_pull(task_ids="compute_partition")
    print(f"NYC Taxi ETL run | Partition: {partition['year']}-{partition['month']}")
    print(f"Project: {PROJECT_ID} | Cluster: {CLUSTER}")


with DAG(
    dag_id="nyc_taxi_monthly_etl",
    description="Monthly PySpark ETL: NYC Taxi → GCS → Dataproc → BigQuery",
    schedule_interval="0 6 2 * *",    # 6 AM on 2nd of every month
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["data-engineering", "pyspark", "bigquery", "nyc-taxi"],
    max_active_runs=1,
) as dag:

    compute_partition_task = PythonOperator(
        task_id="compute_partition",
        python_callable=compute_partition,
    )

    log_info = PythonOperator(
        task_id="log_run_info",
        python_callable=log_run_info,
    )

    run_pyspark_etl = DataprocSubmitPySparkJobOperator(
        task_id="run_pyspark_etl",
        main=PYSPARK_URI,
        arguments=[
            "--project", PROJECT_ID,
            "--bucket", BUCKET,
            "--year", "{{ ti.xcom_pull(task_ids='compute_partition')['year'] }}",
            "--month", "{{ ti.xcom_pull(task_ids='compute_partition')['month'] }}",
            "--mode", "append",
        ],
        cluster_name=CLUSTER,
        region=REGION,
        project_id=PROJECT_ID,
        dataproc_jars=[BQ_JAR],
        job_name="nyc_taxi_etl_{{ ds_nodash }}",
    )

    # DAG flow
    compute_partition_task >> log_info >> run_pyspark_etl
