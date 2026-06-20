import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Make the src/ modules importable from the DAG worker process.
# Airflow workers execute DAG files from the dags/ directory, so the project
# root is not on sys.path by default.
_SRC = str(Path(__file__).parent.parent / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

# ── Callables ─────────────────────────────────────────────────────────────────
# Imports are intentionally lazy (inside each callable) so that module-level
# code in the pipeline scripts — particularly DATABASE_URL = os.environ[...] —
# runs at task execution time rather than during DAG parsing.

def run_ingest() -> None:
    from ingest import ingest
    ingest()


def run_validate() -> None:
    from validate import validate
    validate()


def run_analytics_and_forecast() -> None:
    from analytics import run_analytics
    from forecast import run as run_forecast
    run_analytics()
    run_forecast()


# ── DAG ───────────────────────────────────────────────────────────────────────

_default_args = {
    "owner": "nem-analytics",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nem_daily_pipeline",
    description="Ingest P5MIN data, validate, run analytics and forecast.",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    default_args=_default_args,
    tags=["nem", "analytics"],
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_task",
        python_callable=run_ingest,
    )

    validate_task = PythonOperator(
        task_id="validate_task",
        python_callable=run_validate,
    )

    analytics_task = PythonOperator(
        task_id="analytics_task",
        python_callable=run_analytics_and_forecast,
    )

    ingest_task >> validate_task >> analytics_task
