"""
Lab 3 - Daily Refresh DAG for Cloud Composer
Updates key pages daily: About, Careers, Blog, News
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from pathlib import Path
import json
import sys
import os

sys.path.insert(0, '/home/airflow/gcs/dags')

from src.lab1_scraper import Lab1Scraper
from src.news_intelligence_scraper import NewsIntelligenceScraper
from src.structured_extractor import StructuredExtractor
from src.vector_db_pinecone import PineconeVectorDB
from google.cloud import storage

default_args = {
    'owner': 'quanta',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['alerts@quantacapital.com'],  # Update with your email
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}


def load_companies(**context):
    """Load company list"""
    client = storage.Client()
    bucket_name = os.getenv('DATA_BUCKET', 'pe-dashboard-477417-pe-dashboard-data')
    bucket = client.bucket(bucket_name)
    blob = bucket.blob('forbes_ai50_seed.json')
    
    if blob.exists():
        companies = json.loads(blob.download_as_text())
    else:
        with open('/home/airflow/gcs/dags/data/forbes_ai50_seed.json', 'r') as f:
            companies = json.load(f)
    
    context['ti'].xcom_push(key='companies', value=companies)
    print(f"✅ Loaded {len(companies)} companies")
    return len(companies)


def refresh_key_pages(**context):
    """Re-scrape About, Careers, Blog pages"""
    
    print("=" * 80)
    print("DAILY REFRESH: KEY PAGES")
    print("=" * 80)
    
    ti = context['ti']
    companies = ti.xcom_pull(key='companies', task_ids='load_companies')
    
    # Use faster scraper for daily updates
    scraper = Lab1Scraper(output_dir="/tmp/data/raw", delay=1.0)
    
    for i, company in enumerate(companies, 1):
        print(f"\n[{i}/{len(companies)}] {company['company_name']}")
        try:
            scraper.scrape_company(company)
        except Exception as e:
            print(f"  ❌ Error: {str(e)[:80]}")
    
    print("\n✅ Website refresh complete")


def refresh_news(**context):
    """Get latest news articles"""
    
    print("=" * 80)
    print("DAILY REFRESH: NEWS ARTICLES")
    print("=" * 80)
    
    ti = context['ti']
    companies = ti.xcom_pull(key='companies', task_ids='load_companies')
    
    scraper = NewsIntelligenceScraper(output_dir="/tmp/data/raw")
    
    for i, company in enumerate(companies, 1):
        print(f"\n[{i}/{len(companies)}] {company['company_name']}")
        
        try:
            github = company.get('github') or company['company_id'].replace('-', '')
            linkedin = company.get('linkedin')
            
            scraper.scrape_company(
                company_id=company['company_id'],
                company_name=company['company_name'],
                github_username=github,
                linkedin_url=linkedin,
                max_articles_per_source=2  # Only 2 latest articles per source
            )
        
        except Exception as e:
            print(f"  ❌ Error: {str(e)[:80]}")
        
        import time
        time.sleep(2)
    
    print("\n✅ News refresh complete")


def update_structured_data(**context):
    """Re-extract structured data"""
    
    print("=" * 80)
    print("UPDATING STRUCTURED DATA")
    print("=" * 80)
    
    extractor = StructuredExtractor(model="gpt-4o-mini")
    output_dir = Path("/tmp/data/structured")
    extractor.extract_all_companies(output_dir=output_dir)
    
    print("✅ Extraction complete")


def sync_to_gcs(**context):
    """Upload updated data to GCS"""
    
    print("=" * 80)
    print("SYNCING TO GCS")
    print("=" * 80)
    
    client = storage.Client()
    bucket_name = os.getenv('DATA_BUCKET', 'pe-dashboard-477417-pe-dashboard-data')
    bucket = client.bucket(bucket_name)
    
    structured_dir = Path("/tmp/data/structured")
    
    if not structured_dir.exists():
        print("⚠️  No data to upload")
        return
    
    uploaded = 0
    for json_file in structured_dir.glob("*.json"):
        try:
            blob = bucket.blob(f'structured/{json_file.name}')
            blob.upload_from_filename(str(json_file))
            uploaded += 1
        except Exception as e:
            print(f"❌ {json_file.name}: {e}")
    
    print(f"\n✅ Uploaded {uploaded} files")
    
    # Cleanup
    import shutil
    shutil.rmtree("/tmp/data", ignore_errors=True)


def update_pinecone_incremental(**context):
    """Update Pinecone with changed data only"""
    
    print("=" * 80)
    print("UPDATING PINECONE")
    print("=" * 80)
    
    # For simplicity, rebuild entire index
    # TODO: Implement incremental updates for better performance
    builder = PineconeVectorDB()
    builder.build_for_all_companies()
    
    print("✅ Pinecone updated")


# Define the DAG
with DAG(
    dag_id='ai50_daily_refresh',
    default_args=default_args,
    description='Daily refresh of Forbes AI 50: Key pages + News → GCS',
    schedule_interval='0 3 * * *',  # 3 AM UTC daily
    start_date=days_ago(1),
    catchup=False,
    tags=['ai50', 'production', 'daily'],
    doc_md="""
    ## PE Dashboard - Daily Refresh Pipeline
    
    This DAG refreshes data daily for all Forbes AI 50 companies.
    
    **Pipeline Steps:**
    1. Load company list
    2. Re-scrape key pages (About, Careers, Blog)
    3. Get latest news articles
    4. Update structured data with LLM
    5. Upload to GCS
    6. Update Pinecone index
    
    **Schedule:** Daily at 3:00 AM UTC
    
    **Runtime:** ~1-2 hours
    
    **Cost:** ~$5-8 per run (OpenAI API)
    """,
) as dag:
    
    task_load = PythonOperator(
        task_id='load_companies',
        python_callable=load_companies,
    )
    
    task_refresh_pages = PythonOperator(
        task_id='refresh_key_pages',
        python_callable=refresh_key_pages,
    )
    
    task_refresh_news = PythonOperator(
        task_id='refresh_news',
        python_callable=refresh_news,
    )
    
    task_update_data = PythonOperator(
        task_id='update_structured_data',
        python_callable=update_structured_data,
    )
    
    task_sync = PythonOperator(
        task_id='sync_to_gcs',
        python_callable=sync_to_gcs,
    )
    
    task_pinecone = PythonOperator(
        task_id='update_pinecone',
        python_callable=update_pinecone_incremental,
    )
    
    # Define task dependencies
    task_load >> [task_refresh_pages, task_refresh_news] >> task_update_data >> task_sync >> task_pinecone