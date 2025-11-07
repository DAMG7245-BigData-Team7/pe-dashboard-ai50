"""
Lab 2 - Full Ingestion DAG for Cloud Composer
Scrapes all Forbes AI 50 companies: Websites + News + APIs → GCS + Pinecone
Includes RAW data upload to satisfy assignment requirements
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
from src.utils import save_json
from google.cloud import storage

# DAG default arguments
default_args = {
    'owner': 'quanta',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,  # No retries for expensive operations
    'retry_delay': timedelta(minutes=5),
}


def load_companies(**context):
    """Task 1: Load Forbes AI 50 company list"""
    
    print("=" * 80, flush=True)
    print("TASK 1: LOADING COMPANIES", flush=True)
    print("=" * 80, flush=True)
    
    try:
        # Try to load from GCS first
        client = storage.Client()
        bucket_name = os.getenv('DATA_BUCKET', 'pe-dashboard-477417-pe-dashboard-data')
        bucket = client.bucket(bucket_name)
        blob = bucket.blob('forbes_ai50_seed.json')
        
        if blob.exists():
            companies = json.loads(blob.download_as_text())
            print(f"✅ Loaded from GCS: {bucket_name}/forbes_ai50_seed.json", flush=True)
        else:
            # Fallback to DAGs folder
            with open('/home/airflow/gcs/dags/data/forbes_ai50_seed.json', 'r') as f:
                companies = json.load(f)
            print(f"✅ Loaded from DAGs folder", flush=True)
        
        context['ti'].xcom_push(key='companies', value=companies)
        print(f"✅ Total companies: {len(companies)}", flush=True)
        print("=" * 80, flush=True)
        
        return len(companies)
    
    except Exception as e:
        print(f"❌ Error loading companies: {e}", flush=True)
        raise


def scrape_websites(**context):
    """Task 2: Scrape company websites with lab1_scraper"""
    
    print("\n" + "=" * 80, flush=True)
    print("TASK 2: SCRAPING COMPANY WEBSITES", flush=True)
    print("=" * 80, flush=True)
    
    ti = context['ti']
    companies = ti.xcom_pull(key='companies', task_ids='load_companies')
    
    if not companies:
        raise ValueError("No companies loaded from previous task")
    
    print(f"📊 Total companies to scrape: {len(companies)}", flush=True)
    
    # Use /tmp for ephemeral storage
    scraper = Lab1Scraper(output_dir="/tmp/data/raw", delay=1.5)
    
    results = []
    failed = []
    
    for i, company in enumerate(companies, 1):
        company_name = company.get('company_name', 'Unknown')
        
        # Progress checkpoint every 5 companies
        if i % 5 == 0:
            print(f"\n{'='*60}", flush=True)
            print(f"CHECKPOINT: {i}/{len(companies)} completed", flush=True)
            print(f"Success: {len(results) - len(failed)}, Failed: {len(failed)}", flush=True)
            print(f"{'='*60}\n", flush=True)
            
            # Force garbage collection
            import gc
            gc.collect()
        
        print(f"[{i}/{len(companies)}] Scraping {company_name}...", flush=True)
        
        try:
            result = scraper.scrape_company(company)
            results.append(result)
            
            if result.get('total_pages', 0) == 0:
                failed.append(company)
                print(f"  ⚠️  No pages scraped", flush=True)
            else:
                print(f"  ✅ {result.get('total_pages', 0)} pages", flush=True)
        
        except Exception as e:
            print(f"  ❌ Error: {str(e)[:100]}", flush=True)
            failed.append(company)
    
    # Store results for next tasks
    ti.xcom_push(key='scrape_results', value=results)
    ti.xcom_push(key='failed_websites', value=failed)
    
    print("\n" + "=" * 80, flush=True)
    print(f"✅ SCRAPING COMPLETE", flush=True)
    print(f"   Total: {len(companies)}", flush=True)
    print(f"   Successful: {len(results) - len(failed)}", flush=True)
    print(f"   Failed: {len(failed)}", flush=True)
    print("=" * 80, flush=True)


def scrape_news_and_apis(**context):
    """Task 3: Scrape news articles + GitHub/LinkedIn/Crunchbase"""
    
    print("\n" + "=" * 80, flush=True)
    print("TASK 3: SCRAPING NEWS & EXTERNAL APIs", flush=True)
    print("=" * 80, flush=True)
    
    ti = context['ti']
    companies = ti.xcom_pull(key='companies', task_ids='load_companies')
    
    scraper = NewsIntelligenceScraper(output_dir="/tmp/data/raw")
    
    results = []
    
    for i, company in enumerate(companies, 1):
        company_id = company.get('company_id')
        company_name = company.get('company_name')
        
        print(f"[{i}/{len(companies)}] {company_name}", flush=True)
        
        try:
            # Infer GitHub username from company_id
            github = company.get('github') or company_id.replace('-', '')
            linkedin = company.get('linkedin')
            
            result = scraper.scrape_company(
                company_id=company_id,
                company_name=company_name,
                github_username=github,
                linkedin_url=linkedin,
                max_articles_per_source=3
            )
            results.append(result)
        
        except Exception as e:
            print(f"  ❌ Error: {str(e)[:100]}", flush=True)
        
        # Rate limiting
        if i < len(companies):
            import time
            time.sleep(2)
    
    ti.xcom_push(key='news_results', value=results)
    
    print("\n" + "=" * 80, flush=True)
    print(f"✅ NEWS SCRAPING COMPLETE", flush=True)
    print(f"   Companies processed: {len(results)}/{len(companies)}", flush=True)
    print("=" * 80, flush=True)


def store_raw_to_cloud(**context):
    """Task 4: Upload RAW scraped data to GCS (Assignment requirement)"""
    
    print("\n" + "=" * 80, flush=True)
    print("TASK 4: STORING RAW DATA TO GCS", flush=True)
    print("=" * 80, flush=True)
    
    try:
        client = storage.Client()
        bucket_name = os.getenv('DATA_BUCKET', 'pe-dashboard-477417-pe-dashboard-data')
        bucket = client.bucket(bucket_name)
        
        raw_dir = Path("/tmp/data/raw")
        
        if not raw_dir.exists():
            print("⚠️  No raw data directory found", flush=True)
            return
        
        uploaded_files = 0
        uploaded_companies = 0
        
        # Upload all raw data for each company
        for company_folder in sorted(raw_dir.iterdir()):
            if not company_folder.is_dir():
                continue
            
            company_id = company_folder.name
            print(f"\n📤 Uploading raw data for: {company_id}", flush=True)
            
            # Walk through all files in company folder
            for root, dirs, files in os.walk(company_folder):
                for file in files:
                    local_path = Path(root) / file
                    
                    # Calculate relative path from raw_dir
                    relative_path = local_path.relative_to(raw_dir)
                    
                    # Upload to GCS with same structure: raw/{company_id}/...
                    gcs_path = f'raw/{relative_path}'
                    blob = bucket.blob(gcs_path)
                    
                    try:
                        blob.upload_from_filename(str(local_path))
                        uploaded_files += 1
                        
                        # Progress update every 50 files
                        if uploaded_files % 50 == 0:
                            print(f"  Uploaded {uploaded_files} files...", flush=True)
                    
                    except Exception as e:
                        print(f"  ❌ Error uploading {gcs_path}: {str(e)[:60]}", flush=True)
            
            uploaded_companies += 1
            print(f"  ✅ {company_id} complete ({uploaded_companies}/{len(list(raw_dir.iterdir()))})", flush=True)
        
        print("\n" + "=" * 80, flush=True)
        print(f"✅ RAW DATA UPLOAD COMPLETE", flush=True)
        print(f"   Companies: {uploaded_companies}", flush=True)
        print(f"   Files: {uploaded_files}", flush=True)
        print(f"   Bucket: gs://{bucket_name}/raw/", flush=True)
        print("=" * 80, flush=True)
    
    except Exception as e:
        print(f"❌ Raw data upload error: {e}", flush=True)
        import traceback
        traceback.print_exc()
        raise


def extract_structured_data(**context):
    """Task 5: Extract structured data with LLM - BATCHED"""
    
    print("\n" + "=" * 80, flush=True)
    print("TASK 5: STRUCTURED EXTRACTION (BATCHED)", flush=True)
    print("=" * 80, flush=True)
    
    try:
        extractor = StructuredExtractor(model="gpt-4o-mini")
        
        # Get all company folders
        raw_dir = Path("/tmp/data/raw")
        
        if not raw_dir.exists():
            print(f"❌ Raw data directory not found: {raw_dir}", flush=True)
            return
        
        company_folders = sorted([f for f in raw_dir.iterdir() if f.is_dir()])
        
        # Process in batches of 10
        batch_size = 10
        total = len(company_folders)
        
        print(f"📊 Total companies to extract: {total}", flush=True)
        
        for batch_start in range(0, total, batch_size):
            batch_end = min(batch_start + batch_size, total)
            batch = company_folders[batch_start:batch_end]
            
            print(f"\n📦 Batch {batch_start//batch_size + 1}: Companies {batch_start+1}-{batch_end}", flush=True)
            
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
                            
                            print(f"  ✅ {company_id}", flush=True)
                    
                    except Exception as e:
                        print(f"  ❌ {company_id}: {str(e)[:80]}", flush=True)
                
                # Rate limiting between companies
                import time
                time.sleep(3)
            
            # Longer delay between batches to avoid rate limits
            if batch_end < total:
                print(f"\n⏳ Batch complete. Waiting 30s before next batch...", flush=True)
                import time
                time.sleep(30)
        
        print("\n✅ All batches complete", flush=True)
    
    except Exception as e:
        print(f"❌ Extraction error: {e}", flush=True)
        import traceback
        traceback.print_exc()
        raise


def upload_structured_to_gcs(**context):
    """Task 6: Upload STRUCTURED data to GCS bucket"""
    
    print("\n" + "=" * 80, flush=True)
    print("TASK 6: UPLOADING STRUCTURED DATA TO GCS", flush=True)
    print("=" * 80, flush=True)
    
    try:
        client = storage.Client()
        bucket_name = os.getenv('DATA_BUCKET', 'pe-dashboard-477417-pe-dashboard-data')
        bucket = client.bucket(bucket_name)
        
        structured_dir = Path("/tmp/data/structured")
        
        if not structured_dir.exists():
            print("⚠️  No structured data directory found", flush=True)
            return
        
        uploaded = 0
        errors = 0
        
        for json_file in structured_dir.glob("*.json"):
            try:
                blob = bucket.blob(f'structured/{json_file.name}')
                blob.upload_from_filename(str(json_file))
                uploaded += 1
                print(f"  ✅ {json_file.name}", flush=True)
            except Exception as e:
                errors += 1
                print(f"  ❌ {json_file.name}: {str(e)[:60]}", flush=True)
        
        print("\n" + "=" * 80, flush=True)
        print(f"✅ STRUCTURED DATA UPLOAD COMPLETE", flush=True)
        print(f"   Uploaded: {uploaded} files", flush=True)
        print(f"   Errors: {errors}", flush=True)
        print(f"   Bucket: gs://{bucket_name}/structured/", flush=True)
        print("=" * 80, flush=True)
        
        # Clean up /tmp to save disk space
        import shutil
        shutil.rmtree("/tmp/data", ignore_errors=True)
        print("🧹 Cleaned up /tmp/data", flush=True)
    
    except Exception as e:
        print(f"❌ Upload error: {e}", flush=True)
        raise


def update_pinecone(**context):
    """Task 7: Update Pinecone vector database"""
    
    print("\n" + "=" * 80, flush=True)
    print("TASK 7: UPDATING PINECONE VECTOR DB", flush=True)
    print("=" * 80, flush=True)
    
    try:
        builder = PineconeVectorDB()
        builder.build_for_all_companies(force_recreate=False)  # Use existing index
        
        print("=" * 80, flush=True)
        print("✅ PINECONE UPDATE COMPLETE", flush=True)
        print("=" * 80, flush=True)
    
    except Exception as e:
        print(f"❌ Pinecone update error: {e}", flush=True)
        import traceback
        traceback.print_exc()
        raise


# ============================================================================
# DEFINE THE DAG
# ============================================================================

with DAG(
    dag_id='ai50_full_ingest',
    default_args=default_args,
    description='Full ingestion of Forbes AI 50: Websites + News + APIs → GCS (raw + structured) + Pinecone',
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
    4. Upload RAW data to GCS (HTML, TXT, metadata, news)
    5. Extract structured data using LLM (OpenAI + Instructor)
    6. Upload STRUCTURED data to GCS (JSON payloads)
    7. Update Pinecone vector database for RAG pipeline
    
    **Runtime:** ~3-4 hours for all 52 companies
    
    **Outputs:**
    - `gs://{DATA_BUCKET}/raw/<company_id>/...` - Raw scraped data
    - `gs://{DATA_BUCKET}/structured/*.json` - Structured company data
    - Pinecone index updated with text chunks
    
    **Cost Estimate:** ~$10-15 per run (OpenAI API costs)
    """,
) as dag:
    
    # ========================================
    # TASK 1: Load Companies
    # ========================================
    task_load = PythonOperator(
        task_id='load_companies',
        python_callable=load_companies,
        doc_md="Load Forbes AI 50 company list from GCS or DAGs folder",
    )
    
    # ========================================
    # TASK 2: Scrape Websites
    # ========================================
    task_scrape_websites = PythonOperator(
        task_id='scrape_websites',
        python_callable=scrape_websites,
        execution_timeout=timedelta(hours=4),  # Long timeout for all companies
        doc_md="Scrape company websites using lab1_scraper (cascade: Requests → Selenium → Playwright)",
    )
    
    # ========================================
    # TASK 3: Scrape News + APIs
    # ========================================
    task_scrape_news = PythonOperator(
        task_id='scrape_news_and_apis',
        python_callable=scrape_news_and_apis,
        execution_timeout=timedelta(hours=2),
        doc_md="Scrape news articles and external APIs (GitHub, LinkedIn, Crunchbase)",
    )
    
    # ========================================
    # TASK 4: Store RAW Data to GCS (NEW!)
    # ========================================
    task_store_raw = PythonOperator(
        task_id='store_raw_to_cloud',
        python_callable=store_raw_to_cloud,
        execution_timeout=timedelta(minutes=30),
        doc_md="Upload RAW scraped data (HTML, TXT, metadata, news) to GCS",
    )
    
    # ========================================
    # TASK 5: Extract Structured Data
    # ========================================
    task_extract = PythonOperator(
        task_id='extract_structured_data',
        python_callable=extract_structured_data,
        execution_timeout=timedelta(hours=3),
        retries=0,
        doc_md="Extract structured Pydantic objects using OpenAI + Instructor",
    )
    
    # ========================================
    # TASK 6: Upload Structured Data to GCS
    # ========================================
    task_upload = PythonOperator(
        task_id='upload_structured_to_gcs',
        python_callable=upload_structured_to_gcs,
        doc_md="Upload structured JSON files to GCS bucket for Cloud Run app",
    )
    
    # ========================================
    # TASK 7: Update Pinecone
    # ========================================
    task_pinecone = PythonOperator(
        task_id='update_pinecone',
        python_callable=update_pinecone,
        execution_timeout=timedelta(hours=1),
        doc_md="Update Pinecone vector database with embedded text chunks",
    )
    
    # ========================================
    # DEFINE TASK DEPENDENCIES
    # ========================================
    task_load >> task_scrape_websites >> task_scrape_news >> task_store_raw >> task_extract >> task_upload >> task_pinecone