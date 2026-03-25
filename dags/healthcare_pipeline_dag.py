"""
healthcare_pipeline_dag.py — Airflow DAG
Healthcare ETL Pipeline | Anuprita Dhamale

Schedule: Daily at 2 AM UTC
Retries: 2 (with 5-minute delay)
Alerts: Email on failure
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.email import send_email

import sys
sys.path.insert(0, "/opt/airflow/src")

from extract   import S3Extractor
from transform import transform_claims, transform_patients, transform_providers, build_revenue_mart
from load      import SnowflakeLoader

# ── Config ────────────────────────────────────────────────────────────────────
S3_BUCKET = "my-healthcare-bucket"

default_args = {
    "owner":            "anuprita.dhamale",
    "depends_on_past":  False,
    "email":            ["dhamaleanuprita15@gmail.com"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}


# ── Task Functions ─────────────────────────────────────────────────────────────
def extract_task(**context):
    extractor = S3Extractor(bucket=S3_BUCKET)
    for dataset in ["claims", "patients", "providers"]:
        df, metrics = extractor.extract(dataset)
        df.to_parquet(f"/tmp/{dataset}_raw.parquet", index=False)
        context["ti"].xcom_push(key=f"{dataset}_metrics", value=metrics)


def transform_task(**context):
    import pandas as pd
    claims_raw    = pd.read_parquet("/tmp/claims_raw.parquet")
    patients_raw  = pd.read_parquet("/tmp/patients_raw.parquet")
    providers_raw = pd.read_parquet("/tmp/providers_raw.parquet")

    claims    = transform_claims(claims_raw)
    patients  = transform_patients(patients_raw)
    providers = transform_providers(providers_raw)
    mart      = build_revenue_mart(claims, patients, providers)

    claims.to_parquet("/tmp/fact_claims.parquet", index=False)
    patients.to_parquet("/tmp/dim_patient.parquet", index=False)
    providers.to_parquet("/tmp/dim_provider.parquet", index=False)
    mart.to_parquet("/tmp/mart_revenue.parquet", index=False)


def load_task(**context):
    import pandas as pd
    loader = SnowflakeLoader()

    tables = {
        "FACT_CLAIMS":    pd.read_parquet("/tmp/fact_claims.parquet"),
        "DIM_PATIENT":    pd.read_parquet("/tmp/dim_patient.parquet"),
        "DIM_PROVIDER":   pd.read_parquet("/tmp/dim_provider.parquet"),
        "MART_REVENUE":   pd.read_parquet("/tmp/mart_revenue.parquet"),
    }
    for table, df in tables.items():
        loader.load(df, table_name=table)


def quality_gate(**context):
    """Fail the DAG if critical quality thresholds are breached."""
    import pandas as pd
    claims = pd.read_parquet("/tmp/fact_claims.parquet")

    null_pct = claims.isnull().sum().sum() / claims.size * 100
    if null_pct > 5.0:
        raise ValueError(f"Quality gate FAILED: null_pct={null_pct:.2f}% exceeds threshold of 5%")

    if len(claims) == 0:
        raise ValueError("Quality gate FAILED: claims table is empty after transform")

    print(f"✅ Quality gate passed — {len(claims):,} rows, null_pct={null_pct:.2f}%")


# ── DAG Definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="healthcare_etl_pipeline",
    default_args=default_args,
    description="Daily ETL: S3 → Transform → Snowflake for healthcare analytics",
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["healthcare", "etl", "snowflake"],
) as dag:

    start = DummyOperator(task_id="start")

    extract = PythonOperator(
        task_id="extract_from_s3",
        python_callable=extract_task,
        provide_context=True,
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_task,
        provide_context=True,
    )

    quality = PythonOperator(
        task_id="quality_gate",
        python_callable=quality_gate,
        provide_context=True,
    )

    load = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_task,
        provide_context=True,
    )

    end = DummyOperator(task_id="end")

    start >> extract >> transform >> quality >> load >> end
