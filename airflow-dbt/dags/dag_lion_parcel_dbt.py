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
    # dbt deps sudah ter-install saat build image; jangan diulang tiap task run.
    install_dbt_deps=False,
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
    # Menggunakan python os, bisa di ganti dengna subproses untuk berjalan di env sendiri
    invocation_mode=InvocationMode.DBT_RUNNER,
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

    def _check_new_data(dag_run, **kwargs):
        """Gerbang efisiensi: jalankan dbt hanya bila ada baris yang belum mendarat di Bronze.

        Pembandingnya adalah watermark warehouse (max updatedAt di Bronze), BUKAN awal
        window jadwal. Membandingkan ke window jadwal menanyakan hal yang keliru: ia
        bertanya "adakah data baru dalam satu jam terakhir?", padahal yang relevan adalah
        "adakah data yang belum saya punya?". Keduanya berbeda ketika Bronze masih kosong
        sementara Postgres sudah berisi data lama -- kasus itu membuat seluruh data awal
        tidak pernah tertarik, tanpa satu pun error.
        """
        from airflow.sdk import BaseHook
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from clickhouse_driver import Client

        # Trigger manual dari UI selalu dijalankan (untuk demo, backfill, dan debugging).
        if dag_run.run_type == "manual":
            print("Trigger manual: pengecekan dilewati, pipeline tetap dijalankan.")
            return True

        conn = BaseHook.get_connection("clickhouse_default")
        client = Client(
            host=conn.host,
            port=conn.port or 9000,
            user=conn.login,
            password=conn.password or "",
            database=conn.schema or "default",
        )
        try:
            watermark = client.execute(
                "SELECT max(updatedAt) FROM bronze_lion.bronze_retail_transactions"
            )[0][0]
        except Exception as exc:
            # Tabel Bronze belum ada = run pertama. Wajib jalan agar full load terjadi.
            print(f"Bronze belum tersedia ({type(exc).__name__}). Menjalankan full load.")
            return True
        finally:
            client.disconnect()

        if watermark is None:
            print("Bronze kosong. Menjalankan full load.")
            return True

        # Sengaja memakai '>' (bukan '>='): pertanyaannya adalah "adakah yang LEBIH BARU
        # dari yang sudah saya punya?". Dengan '>=' gerbang tidak akan pernah men-skip,
        # karena baris di watermark itu sendiri selalu ada. Baris yang ter-commit pada
        # milidetik yang sama persis tetap aman: model Bronze memakai '>=' sehingga
        # menarik ulang batas tersebut pada run berikutnya.
        hook = PostgresHook(postgres_conn_id="postgres_default")
        records = hook.get_records(
            'SELECT 1 FROM retail_transactions WHERE "updatedAt" > %s LIMIT 1',
            parameters=(watermark,),
        )

        if not records:
            print(f"Tidak ada baris lebih baru dari watermark {watermark}. SKIP.")
            return False

        print(f"Ada baris lebih baru dari watermark {watermark}. Menjalankan dbt.")
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
            emit_datasets=False,
            test_behavior=TestBehavior.AFTER_EACH,
        ),
    )

    end = EmptyOperator(task_id="end")

    start >> check_new_data >> dbt_run >> end
