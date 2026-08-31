"""Uji integritas DAG — statis, tanpa menyentuh Postgres/ClickHouse.

Memvalidasi seluruh DagBag: import bersih, konvensi proyek (tags, retries,
catchup, paused, owner, callback, timezone), dan yang terpenting bagi arsitektur
ini — bahwa TaskGroup Cosmos benar-benar memuat seluruh model dbt yang ada di
disk (deteksi manifest.json basi).

Jalankan:
    docker exec -u airflow lion_airflow pytest /opt/airflow/tests/dags/ -v
"""

import os
import logging
from contextlib import contextmanager
from pathlib import Path

import pytest
from airflow.models import DagBag

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
DBT_MODELS_DIR = Path(AIRFLOW_HOME) / "dbt" / "lion_parcel_project" / "models"

# DAG yang wajib ada. Rename tanpa memperbarui daftar ini = test merah,
# karena dag_id dirujuk oleh dokumentasi dan prosedur operasional.
EXPECTED_DAGS = {"generate_retail_transactions", "lion_parcel_dbt_pipeline"}

DBT_TASK_GROUP = "dbt_process"


@contextmanager
def suppress_logging(namespace):
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


# DagBag mahal untuk dibangun — parse sekali, pakai ulang di semua test.
with suppress_logging("airflow"):
    DAG_BAG = DagBag(os.path.join(AIRFLOW_HOME, "dags"))


def _rel(path):
    return os.path.relpath(path, AIRFLOW_HOME)


def _dags():
    return [(dag_id, dag, _rel(dag.fileloc)) for dag_id, dag in DAG_BAG.dags.items()]


_IDS = [d[2] for d in _dags()]


# ---------------------------------------------------------------------------
# Import & registrasi
# ---------------------------------------------------------------------------

def test_no_import_errors():
    """Tidak ada file DAG yang gagal di-import.

    Ini juga menangkap manifest.json yang hilang: Cosmos me-render TaskGroup saat
    parse, jadi manifest tak terbaca akan muncul sebagai import error di sini.
    """
    errors = {_rel(k): v.strip() for k, v in DAG_BAG.import_errors.items()}
    assert not errors, f"DAG gagal import:\n{errors}"


def test_dagbag_not_empty():
    """DagBag benar-benar memuat DAG (bukan folder kosong / path salah)."""
    assert DAG_BAG.dags, "tidak ada DAG yang terdeteksi di DagBag"


def test_expected_dags_registered():
    """Kedua DAG produksi terdaftar dengan dag_id yang persis sesuai dokumentasi."""
    actual = set(DAG_BAG.dags)
    assert EXPECTED_DAGS <= actual, (
        f"DAG hilang atau berganti nama: {EXPECTED_DAGS - actual}"
    )


# ---------------------------------------------------------------------------
# Konvensi yang berlaku untuk SEMUA DAG
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("dag_id,dag,fileloc", _dags(), ids=_IDS)
def test_dag_has_tags(dag_id, dag, fileloc):
    assert dag.tags, f"{dag_id} ({fileloc}) tidak punya tags"


@pytest.mark.parametrize("dag_id,dag,fileloc", _dags(), ids=_IDS)
def test_dag_retries(dag_id, dag, fileloc):
    retries = dag.default_args.get("retries")
    assert retries is not None and retries >= 2, (
        f"{dag_id} ({fileloc}) harus punya retries >= 2 (sekarang: {retries})"
    )


@pytest.mark.parametrize("dag_id,dag,fileloc", _dags(), ids=_IDS)
def test_dag_has_retry_delay(dag_id, dag, fileloc):
    assert dag.default_args.get("retry_delay"), (
        f"{dag_id} ({fileloc}) tidak punya retry_delay"
    )


@pytest.mark.parametrize("dag_id,dag,fileloc", _dags(), ids=_IDS)
def test_dag_catchup_disabled(dag_id, dag, fileloc):
    # Pipeline hourly berbasis watermark: catchup harus mati agar tidak backfill liar.
    assert dag.catchup is False, f"{dag_id} ({fileloc}) harus catchup=False"


@pytest.mark.parametrize("dag_id,dag,fileloc", _dags(), ids=_IDS)
def test_dag_has_start_date(dag_id, dag, fileloc):
    assert dag.start_date is not None, f"{dag_id} ({fileloc}) tidak punya start_date"


@pytest.mark.parametrize("dag_id,dag,fileloc", _dags(), ids=_IDS)
def test_dag_paused_on_creation(dag_id, dag, fileloc):
    # DAG dibuat dalam keadaan paused; di-unpause manual saat siap.
    assert dag.is_paused_upon_creation is True, (
        f"{dag_id} ({fileloc}) harus is_paused_upon_creation=True"
    )


@pytest.mark.parametrize("dag_id,dag,fileloc", _dags(), ids=_IDS)
def test_dag_has_owner(dag_id, dag, fileloc):
    assert dag.default_args.get("owner"), f"{dag_id} ({fileloc}) tidak punya owner"


@pytest.mark.parametrize("dag_id,dag,fileloc", _dags(), ids=_IDS)
def test_dag_timezone_jakarta(dag_id, dag, fileloc):
    # Kontrak proyek: seluruh penjadwalan memakai WIB, sejalan dengan Postgres & ClickHouse.
    assert "Asia/Jakarta" in str(dag.timezone), (
        f"{dag_id} ({fileloc}) harus bertimezone Asia/Jakarta (sekarang: {dag.timezone})"
    )


@pytest.mark.parametrize("dag_id,dag,fileloc", _dags(), ids=_IDS)
def test_dag_has_alerting_callbacks(dag_id, dag, fileloc):
    # Notifikasi Telegram sukses/gagal wajib: pipeline berjalan tanpa ditonton.
    assert dag.on_failure_callback, f"{dag_id} ({fileloc}) tidak punya on_failure_callback"
    assert dag.on_success_callback, f"{dag_id} ({fileloc}) tidak punya on_success_callback"


# ---------------------------------------------------------------------------
# Kontrak khusus pipeline dbt (Cosmos)
# ---------------------------------------------------------------------------

def _dbt_dag():
    dag = DAG_BAG.dags.get("lion_parcel_dbt_pipeline")
    if dag is None:
        pytest.fail("DAG lion_parcel_dbt_pipeline tidak ditemukan")
    return dag


def _dbt_model_names():
    """Nama seluruh model dbt di disk (bronze/silver/gold)."""
    if not DBT_MODELS_DIR.is_dir():
        pytest.skip(f"direktori model dbt tidak ter-mount: {DBT_MODELS_DIR}")
    return {p.stem for p in DBT_MODELS_DIR.rglob("*.sql")}


def _downstream_ids(dag, task_id):
    """Seluruh task_id di hilir task_id (BFS, lintas-versi Airflow)."""
    seen, queue = set(), [task_id]
    while queue:
        current = queue.pop()
        for nxt in dag.get_task(current).downstream_task_ids:
            if nxt not in seen:
                seen.add(nxt)
                queue.append(nxt)
    return seen


def test_dbt_taskgroup_covers_every_model():
    """Setiap model dbt di disk punya task 'run' di dalam TaskGroup Cosmos.

    Ini penjaga jebakan paling mahal di arsitektur ini: manifest.json dibangun saat
    'docker compose build'. Menambah model lalu hanya mengandalkan bind mount membuat
    model itu TIDAK PERNAH dijalankan, tanpa satu pun error. Test ini yang menangkapnya.
    """
    dag = _dbt_dag()
    task_ids = {t.task_id for t in dag.tasks}
    missing = {
        model for model in _dbt_model_names()
        if f"{DBT_TASK_GROUP}.{model}.run" not in task_ids
    }
    assert not missing, (
        "Model dbt berikut ada di disk tapi tidak ada task-nya di DAG "
        f"(manifest.json basi -- jalankan 'docker compose build'): {sorted(missing)}"
    )


def test_dbt_models_are_gated_by_short_circuit():
    """Seluruh task dbt berada di hilir gerbang check_new_data.

    Bila gerbang ini terputus, pipeline membangun ulang lima tabel Gold setiap jam
    walau tidak ada data baru sama sekali.
    """
    dag = _dbt_dag()
    downstream = _downstream_ids(dag, "check_new_data")
    dbt_tasks = {t.task_id for t in dag.tasks if t.task_id.startswith(f"{DBT_TASK_GROUP}.")}
    assert dbt_tasks, "tidak ada task dbt yang ter-render dari manifest"
    assert dbt_tasks <= downstream, (
        f"task dbt ini tidak melewati gerbang check_new_data: {sorted(dbt_tasks - downstream)}"
    )


def test_dbt_models_have_test_tasks():
    """TestBehavior.AFTER_EACH aktif: tiap model yang dijalankan diikuti task test.

    Menjamin data quality benar-benar dieksekusi di pipeline, bukan hanya
    terdefinisi di schema.yml.
    """
    dag = _dbt_dag()
    task_ids = {t.task_id for t in dag.tasks}
    run_models = {
        t.split(".")[1] for t in task_ids
        if t.startswith(f"{DBT_TASK_GROUP}.") and t.endswith(".run")
    }
    without_test = {
        m for m in run_models if f"{DBT_TASK_GROUP}.{m}.test" not in task_ids
    }
    assert not without_test, (
        f"model dijalankan tanpa task test menyertainya: {sorted(without_test)}"
    )
