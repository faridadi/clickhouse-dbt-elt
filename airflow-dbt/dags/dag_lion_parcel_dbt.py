from datetime import timedelta
import os
import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from cosmos import (
    DbtTaskGroup,
    ProfileConfig,
    RenderConfig,
    LoadMode,
    TestBehavior,
    ProjectConfig,
    ExecutionConfig,
    InvocationMode
)
from cosmos.profiles import ClickhouseUserPasswordProfileMapping

# Fallback ke Dummy callback jika telegram_sender belum ada (untuk testing)
try:
    from include.telegram_sender import send_failed_message, send_success_message
except ImportError:
    send_failed_message = None
    send_success_message = None

# dbt configuration paths
PATH_DBT_PROJECT = f"{os.environ.get('AIRFLOW_HOME', '/opt/airflow')}/dbt/lion_parcel_project"

# Manifest dibangun saat 'docker compose build' (lihat Dockerfile) dan disimpan di luar
# /opt/airflow/dbt agar tidak tertimpa bind mount dari host.
lion_project_config = ProjectConfig(
    dbt_project_path=PATH_DBT_PROJECT,
    manifest_path="/opt/airflow/dbt_manifest/manifest.json",
)

# Profile Config menggunakan ProfileMapping ke Airflow Connection
lion_profile_config = ProfileConfig(
    profile_name="lion_dwh",
    target_name="prod",
    profile_mapping=ClickhouseUserPasswordProfileMapping(
        conn_id="clickhouse_default",
        profile_args={
            "schema": "lion_parcel_base",
        },
    ),
)

lion_execution_config = ExecutionConfig(
    dbt_executable_path="dbt",
    invocation_mode=InvocationMode.SUBPROCESS,
)

with DAG(
    dag_id="lion_parcel_dbt_pipeline",
    description="DAG untuk menjalankan dbt pipeline Lion Parcel (Bronze -> Silver -> Gold)",
    default_args={
        "owner": "admin",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    schedule="@hourly",
    tags=["lion_parcel", "dbt", "elt"],
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Jakarta"),
    catchup=False,
    is_paused_upon_creation=True,
    on_failure_callback=send_failed_message,
    on_success_callback=send_success_message,
) as dag:

    start = EmptyOperator(task_id="start")

    def _check_new_data(data_interval_start, dag_run, **kwargs):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        # Jika DAG di-trigger manual dari UI, bypass pengecekan agar tidak ter-skip
        if dag_run.run_type == "manual":
            print("DAG dijalankan secara manual (Trigger DAG). Bypass pengecekan waktu (SKIP = FALSE).")
            return True
            
        hook = PostgresHook(postgres_conn_id="postgres_default")
        sql = """
            SELECT 1 
            FROM retail_transactions 
            WHERE "updatedAt" >= %s 
            LIMIT 1
        """
        # Kita cek apakah ada data dengan updatedAt >= batas awal jadwal eksekusi
        records = hook.get_records(sql, parameters=(data_interval_start,))
        
        if not records:
            print("Tidak ada data baru di PostgreSQL. Melewati eksekusi dbt (SKIP).")
            return False
            
        print("Data baru ditemukan! Melanjutkan eksekusi dbt.")
        return True

    from airflow.providers.standard.operators.python import ShortCircuitOperator
    
    check_new_data = ShortCircuitOperator(
        task_id="check_new_data",
        python_callable=_check_new_data,
    )

    dbt_run = DbtTaskGroup(
        group_id="dbt_process",
        project_config=lion_project_config,
        profile_config=lion_profile_config,
        execution_config=lion_execution_config,
        render_config=RenderConfig(
            # Menggunakan manifest.json hasil dari build Dockerfile (Super Cepat!)
            load_method=LoadMode.DBT_MANIFEST,
            dbt_deps=False,
            emit_datasets=False,
            test_behavior=TestBehavior.AFTER_EACH,
        ),
        operator_args={"install_deps": False},
    )

    end = EmptyOperator(task_id="end")

    start >> check_new_data >> dbt_run >> end
