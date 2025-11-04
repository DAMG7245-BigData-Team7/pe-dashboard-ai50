from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def refresh_changed(**context):
    print("Refreshing changed companies...")

with DAG(
    dag_id="ai50_daily_refresh_dag",
    start_date=datetime(2025, 10, 31),
    schedule="0 3 * * *",
    catchup=False,
    tags=["ai50", "orbit", "daily"],
) as dag:

    t1 = PythonOperator(
        task_id="refresh_changed_companies",
        python_callable=refresh_changed,
    )
