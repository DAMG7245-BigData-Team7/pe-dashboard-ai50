"""
Lab 2 - Full Ingestion DAG for Cloud Composer
Scrapes all Forbes AI 50 companies: Websites + News + APIs → GCS + Pinecone
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from pathlib import Path
import json
import sys
import os

# Cloud Composer path configuration
sys.path.insert(0, '/home/airflow/gcs/dags')

from src.lab1_scraper import Lab1Scraper
from src.news_intelligence_scraper import NewsIntelligenceScraper
from src.structured_extractor import StructuredExtractor
from src.vector_db_pinecone import PineconeVectorDB
from google.cloud import storage

# DAG default arguments
default_args = {
    'owner': 'quanta',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def load_companies(**context):
    """Task 1: Load Forbes AI 50 company list"""
    
    print("=" * 80)
    print("TASK 1: LOADING COMPANIES")
    print("=" * 80)
    
    try:
        # Try to load from GCS first
        client = storage.Client()
        bucket_name = os.getenv('DATA_BUCKET', 'pe-dashboard-477417-pe-dashboard-data')
        bucket = client.bucket(bucket_name)
        blob = bucket.blob('forbes_ai50_seed.json')
        
        if blob.exists():
            companies = json.loads(blob.download_as_text())
            print(f"✅ Loaded from GCS: {bucket_name}/forbes_ai50_seed.json")
        else:
            # Fallback to DAGs folder
            with open('/home/airflow/gcs/dags/data/forbes_ai50_seed.json', 'r') as f:
                companies = json.load(f)
            print(f"✅ Loaded from DAGs folder")
        
        context['ti'].xcom_push(key='companies', value=companies)
        print(f"✅ Total companies: {len(companies)}")
        print("=" * 80)
        
        return len(companies)
    
    except Exception as e:
        print(f"❌ Error loading companies: {e}")
        raise


def scrape_websites(**context):
    """Task 2: Scrape company websites with lab1_scraper"""
    
    print("\n" + "=" * 80)
    print("TASK 2: SCRAPING COMPANY WEBSITES")
    print("=" * 80)
    
    ti = context['ti']
    companies = ti.xcom_pull(key='companies', task_ids='load_companies')
    
    if not companies:
        raise ValueError("No companies loaded from previous task")
    
    # Use /tmp for ephemeral storage
    scraper = Lab1Scraper(output_dir="/tmp/data/raw", delay=1.5)
    
    results = []
    failed = []
    
    for i, company in enumerate(companies, 1):
        company_name = company.get('company_name', 'Unknown')
        print(f"\n[{i}/{len(companies)}] Scraping {company_name}...")
        
        try:
            result = scraper.scrape_company(company)
            results.append(result)
            
            if result.get('total_pages', 0) == 0:
                failed.append(company)
                print(f"  ⚠️  No pages scraped")
        
        except Exception as e:
            print(f"  ❌ Error: {str(e)[:100]}")
            failed.append(company)
    
    # Store results for next tasks
    ti.xcom_push(key='scrape_results', value=results)
    ti.xcom_push(key='failed_websites', value=failed)
    
    print("\n" + "=" * 80)
    print(f"✅ SCRAPING COMPLETE")
    print(f"   Successful: {len(results) - len(failed)}/{len(companies)}")
    print(f"   Failed: {len(failed)}")
    print("=" * 80)


def scrape_news_and_apis(**context):
    """Task 3: Scrape news articles + GitHub/LinkedIn/Crunchbase"""
    
    print("\n" + "=" * 80)
    print("TASK 3: SCRAPING NEWS & EXTERNAL APIs")
    print("=" * 80)
    
    ti = context['ti']
    companies = ti.xcom_pull(key='companies', task_ids='load_companies')
    
    scraper = NewsIntelligenceScraper(output_dir="/tmp/data/raw")
    
    results = []
    
    for i, company in enumerate(companies, 1):
        company_id = company.get('company_id')
        company_name = company.get('company_name')
        
        print(f"\n[{i}/{len(companies)}] {company_name}")
        
        try:
            # Infer GitHub username from company_id
            github = company.get('github') or company_id.replace('-', '')
            linkedin = company.get('linkedin')
            
            result = scraper.scrape_company(
                company_id=company_id,
                company_name=company_name,
                github_username=github,
                linkedin_url=linkedin,
                max_articles_per_source=3  # Limit for faster execution
            )
            results.append(result)
        
        except Exception as e:
            print(f"  ❌ Error: {str(e)[:100]}")
        
        # Rate limiting
        if i < len(companies):
            import time
            time.sleep(2)
    
    ti.xcom_push(key='news_results', value=results)
    
    print("\n" + "=" * 80)
    print(f"✅ NEWS SCRAPING COMPLETE")
    print(f"   Companies processed: {len(results)}/{len(companies)}")
    print("=" * 80)


def extract_structured_data(**context):
    """Task 4: Extract structured data with LLM - BATCHED"""
    
    print("\n" + "=" * 80)
    print("TASK 4: STRUCTURED EXTRACTION (BATCHED)")
    print("=" * 80)
    
    try:
        extractor = StructuredExtractor(model="gpt-4o-mini")
        
        # Get all company folders
        raw_dir = Path("/tmp/data/raw")
        company_folders = sorted([f for f in raw_dir.iterdir() if f.is_dir()])
        
        # Process in batches of 10
        batch_size = 10
        total = len(company_folders)
        
        for batch_start in range(0, total, batch_size):
            batch_end = min(batch_start + batch_size, total)
            batch = company_folders[batch_start:batch_end]
            
            print(f"\n📦 Processing batch {batch_start//batch_size + 1}: Companies {batch_start+1}-{batch_end}")
            
            for company_folder in batch:
                company_id = company_folder.name
                
                # Find run folder
                initial_folder = company_folder / "initial"
                run_folder = None
                
                if initial_folder.exists():
                    run_folders = sorted(initial_folder.iterdir(), reverse=True)
                    if run_folders:
                        run_folder = run_folders[0]
                
                if run_folder:
                    try:
                        payload = extractor.extract_company(company_id, run_folder)
                        
                        if payload:
                            output_dir = Path("/tmp/data/structured")
                            output_dir.mkdir(parents=True, exist_ok=True)
                            output_file = output_dir / f"{company_id}.json"
                            
                            payload_dict = payload.model_dump(mode='json')
                            save_json(payload_dict, output_file)
                            
                            print(f"  ✅ {company_id}")
                    
                    except Exception as e:
                        print(f"  ❌ {company_id}: {str(e)[:80]}")
                
                # Rate limiting between companies
                import time
                time.sleep(3)  # 3 seconds between each company
            
            # Longer delay between batches
            if batch_end < total:
                print(f"\n⏳ Batch complete. Waiting 30s before next batch...")
                import time
                time.sleep(30)
        
        print("\n✅ All batches complete")
    
    except Exception as e:
        print(f"❌ Extraction error: {e}")
        import traceback
        traceback.print_exc()
        raise

def upload_to_gcs(**context):
    """Task 5: Upload structured data to GCS bucket"""
    
    print("\n" + "=" * 80)
    print("TASK 5: UPLOADING TO GCS")
    print("=" * 80)
    
    try:
        client = storage.Client()
        bucket_name = os.getenv('DATA_BUCKET', 'pe-dashboard-477417-pe-dashboard-data')
        bucket = client.bucket(bucket_name)
        
        structured_dir = Path("/tmp/data/structured")
        
        if not structured_dir.exists():
            print("⚠️  No structured data directory found")
            return
        
        uploaded = 0
        errors = 0
        
        for json_file in structured_dir.glob("*.json"):
            try:
                blob = bucket.blob(f'structured/{json_file.name}')
                blob.upload_from_filename(str(json_file))
                uploaded += 1
                print(f"  ✅ {json_file.name}")
            except Exception as e:
                errors += 1
                print(f"  ❌ {json_file.name}: {str(e)[:60]}")
        
        print("\n" + "=" * 80)
        print(f"✅ UPLOAD COMPLETE")
        print(f"   Uploaded: {uploaded} files")
        print(f"   Errors: {errors}")
        print(f"   Bucket: gs://{bucket_name}/structured/")
        print("=" * 80)
        
        # Clean up /tmp to save disk space
        import shutil
        shutil.rmtree("/tmp/data", ignore_errors=True)
        print("🧹 Cleaned up /tmp/data")
    
    except Exception as e:
        print(f"❌ Upload error: {e}")
        raise


def update_pinecone(**context):
    """Task 6: Update Pinecone vector database"""
    
    print("\n" + "=" * 80)
    print("TASK 6: UPDATING PINECONE VECTOR DB")
    print("=" * 80)
    
    try:
        builder = PineconeVectorDB()
        builder.build_for_all_companies()
        
        print("=" * 80)
        print("✅ PINECONE UPDATE COMPLETE")
        print("=" * 80)
    
    except Exception as e:
        print(f"❌ Pinecone update error: {e}")
        import traceback
        traceback.print_exc()
        raise


# Define the DAG
with DAG(
    dag_id='ai50_full_ingest',
    default_args=default_args,
    description='Full ingestion of Forbes AI 50: Websites + News + APIs → GCS + Pinecone',
    schedule_interval='@once',  # Manual trigger only
    start_date=days_ago(1),
    catchup=False,
    tags=['ai50', 'production', 'full-load'],
    doc_md="""
    ## PE Dashboard - Full Ingestion Pipeline
    
    This DAG performs a complete data collection cycle for all Forbes AI 50 companies.
    
    **Pipeline Steps:**
    1. Load company list from GCS
    2. Scrape company websites (homepage, about, careers, blog, products)
    3. Scrape news articles + external APIs (GitHub, LinkedIn, Crunchbase)
    4. Extract structured data using LLM (OpenAI + Instructor)
    5. Upload structured JSONs to GCS bucket
    6. Update Pinecone vector database for RAG pipeline
    
    **Runtime:** ~2-3 hours for all 52 companies
    
    **Outputs:**
    - `gs://{DATA_BUCKET}/structured/*.json` - Structured company data
    - Pinecone index updated with text chunks
    
    **Cost Estimate:** ~$10-15 per run (OpenAI API costs)
    """,
) as dag:
    
    # Task 1: Load companies
    task_load = PythonOperator(
        task_id='load_companies',
        python_callable=load_companies,
        doc_md="Load Forbes AI 50 company list from GCS or DAGs folder",
    )
    
    # Task 2: Scrape websites
    task_scrape_websites = PythonOperator(
        task_id='scrape_websites',
        python_callable=scrape_websites,
        doc_md="Scrape company websites using lab1_scraper (cascade: Requests → Selenium → Playwright)",
    )
    
    # Task 3: Scrape news + APIs
    task_scrape_news = PythonOperator(
        task_id='scrape_news_and_apis',
        python_callable=scrape_news_and_apis,
        doc_md="Scrape news articles and external APIs (GitHub, LinkedIn, Crunchbase)",
    )
    
    # Task 4: Extract structured data
    task_extract = PythonOperator(
    task_id='extract_structured_data',
    python_callable=extract_structured_data,
    execution_timeout=timedelta(hours=3),  # ← ADD THIS (default is 6h but be explicit)
    retries=1,  # ← Reduce retries (don't want to waste API calls)
    retry_delay=timedelta(minutes=15),
    )
    
    # Task 5: Upload to GCS
    task_upload = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        doc_md="Upload structured JSON files to GCS bucket for Cloud Run app",
    )
    
    # Task 6: Update Pinecone
    task_pinecone = PythonOperator(
        task_id='update_pinecone',
        python_callable=update_pinecone,
        doc_md="Update Pinecone vector database with embedded text chunks",
    )
    
    # Define task dependencies (linear pipeline)
    task_load >> task_scrape_websites >> task_scrape_news >> task_extract >> task_upload >> task_pinecone