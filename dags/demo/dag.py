from datetime import datetime
from pathlib import Path

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator

from jett.plugins.airflow.operators import JettOperator

with DAG(
    dag_id="demo_jett",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    template_searchpath=str((Path(__file__).parent / "assets").absolute()),
) as dag:
    start = EmptyOperator(task_id="start", dag=dag)
    jett = JettOperator(task_id="jett", tool="duckdb.csv.tool", dag=dag)
    start >> jett
