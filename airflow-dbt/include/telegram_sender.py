from airflow.models import Variable
import pendulum
from airflow.providers.telegram.hooks.telegram import TelegramHook

#! Tambahkan Variable telegram_token dan telegram_chat_id di Airflow UI
#! ATAU Buat Connection 'telegram_default' (Conn Type: Telegram)


def _send_telegram_message(bot_token: str, chat_id: str, message: str = "Hello from Airflow"):
    """
    Sends a message utilizing the Airflow TelegramHook.
    """
    hook = TelegramHook(token=bot_token, chat_id=chat_id)
    hook.send_message({"text": message, "parse_mode": "Markdown"})


def _convert_to_jakarta(dt):
    if dt is None:
        return "-"
    try:
        if not isinstance(dt, pendulum.DateTime):
            dt = pendulum.instance(dt)
        return dt.in_timezone("Asia/Jakarta").strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(dt)


def _format_duration(start, end):
    if start is None:
        return "-"
    if end is None:
        end = pendulum.now("UTC")
    try:
        return str(end - start)
    except Exception:
        return "-"


def _get_failed_task_ids(dag_run):
    # Di Airflow 3, on_failure_callback DAG-level tidak menerima `task_instance`.
    # Ambil daftar task yang gagal langsung dari dag_run.
    try:
        failed_tis = dag_run.get_task_instances(state="failed")
        ids = [ti.task_id for ti in failed_tis]
        return ", ".join(ids) if ids else "-"
    except Exception:
        return "-"


def send_failed_message(context):
    dag = context.get("dag")
    dag_id = dag.dag_id if dag else "-"

    logical_date = context.get("logical_date")
    execution_date = _convert_to_jakarta(logical_date)

    dag_run = context.get("dag_run")
    start = getattr(dag_run, "start_date", None)
    end = getattr(dag_run, "end_date", None)
    failed_tasks = _get_failed_task_ids(dag_run) if dag_run else "-"

    host_url = Variable.get("host_url", "http://0.0.0.0:8080")
    dag_url = f"{host_url}/dags/{dag_id}/grid"

    message = (
        f"🚨 *DAG EXECUTION FAILED* ❌\n"
        f"─────────────────────\n"
        f"🆔 *DAG:* `{dag_id}`\n"
        f"💀 *Failed Task:* `{failed_tasks}`\n"
        f"📅 *Run Date:* `{execution_date}`\n"
        f"⏱️ *Duration:* `{_format_duration(start, end)}`\n"
        f"🌍 *Env:* `{Variable.get('environment', 'dev')}`\n"
        f"🔗 *DAG URL:* [Open Dag]({dag_url})\n"
        f"─────────────────────\n"
        f"🔥 _Check logs immediately!_"
    )
    _send_telegram_message(
        message=message,
        bot_token=Variable.get("telegram_token"),
        chat_id=Variable.get("telegram_chat_id"),
    )


def send_success_message(context):
    dag = context.get("dag")
    dag_id = dag.dag_id if dag else "-"

    logical_date = context.get("logical_date")
    execution_date = _convert_to_jakarta(logical_date)

    dag_run = context.get("dag_run")
    start = getattr(dag_run, "start_date", None)
    end = getattr(dag_run, "end_date", None)

    host_url = Variable.get("host_url", "http://0.0.0.0:8080")
    dag_url = f"{host_url}/dags/{dag_id}/grid"

    message = (
        f"✅ *DAG EXECUTION SUCCESS* 🚀\n"
        f"─────────────────────\n"
        f"🆔 *DAG:* `{dag_id}`\n"
        f"📅 *Run Date:* `{execution_date}`\n"
        f"⏱️ *Duration:* `{_format_duration(start, end)}`\n"
        f"🌍 *Env:* `{Variable.get('environment', 'dev')}`\n"
        f"🔗 *DAG URL:* [Open Dag]({dag_url})\n"
        f"─────────────────────\n"
        f"🎉 _Pipeline completed successfully!_"
    )
    _send_telegram_message(
        message=message,
        bot_token=Variable.get("telegram_token"),
        chat_id=Variable.get("telegram_chat_id"),
    )
