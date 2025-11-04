from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pathlib import Path
import json

DATA_DIR = Path("/opt/airflow/data")

def load_company_list(**context):
    seed_path = DATA_DIR / "forbes_ai50_seed.json"
    companies = json.loads(seed_path.read_text())
    return companies

def scrape_all(**context):
    companies = context["ti"].xcom_pull(task_ids="load_company_list")
    out = []
    for c in companies:
        out.append({"company_name": c["company_name"], "status": "scraped"})
    (DATA_DIR / "raw").mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "raw" / "ingest_log.json").write_text(json.dumps(out, indent=2))

with DAG(
    dag_id="ai50_full_ingest_dag",
    start_date=datetime(2025, 10, 31),
    schedule="@once",
    catchup=False,
    tags=["ai50", "orbit"],
) as dag:

    t1 = PythonOperator(
        task_id="load_company_list",
        python_callable=load_company_list,
    )

    t2 = PythonOperator(
        task_id="scrape_all_companies",
        python_callable=scrape_all,
    )

    t1 >> t2
