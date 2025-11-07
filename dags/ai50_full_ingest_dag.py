"""
Lab 2 - Full Ingestion DAG for Cloud Composer
Scrapes all Forbes AI 50 companies: Websites + News + APIs → GCS + Pinecone
BATCHED VERSION - Processes companies in parallel batches
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
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
    'retries': 0,  # Don't retry batches (expensive)
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


# ============================================================================
# BATCHED SCRAPING - Processes companies in groups of 10
# ============================================================================

def scrape_batch(**context):
    """Scrape a batch of companies"""
    import gc
    
    ti = context['ti']
    batch_num = context['params']['batch_num']
    batch_size = context['params']['batch_size']
    
    companies = ti.xcom_pull(key='companies', task_ids='load_companies')
    
    if not companies:
        raise ValueError("No companies loaded")
    
    # Calculate batch range
    start_idx = (batch_num - 1) * batch_size
    end_idx = min(start_idx + batch_size, len(companies))
    batch = companies[start_idx:end_idx]
    
    print(f"\n{'='*80}", flush=True)
    print(f"BATCH {batch_num}: Companies {start_idx+1} to {end_idx}", flush=True)
    print(f"{'='*80}", flush=True)
    
    scraper = Lab1Scraper(output_dir="/tmp/data/raw", delay=1.5)
    
    results = []
    
    for i, company in enumerate(batch, 1):
        company_name = company.get('company_name', 'Unknown')
        
        print(f"\n[{i}/{len(batch)}] {company_name}", flush=True)
        print(f"Time: {datetime.now().isoformat()}", flush=True)
        
        try:
            result = scraper.scrape_company(company)
            results.append(result)
            print(f"  ✅ Pages: {result.get('total_pages', 0)}", flush=True)
        
        except Exception as e:
            print(f"  ❌ Error: {str(e)[:100]}", flush=True)
            results.append({'company_name': company_name, 'total_pages': 0, 'error': str(e)})
        
        # Cleanup after each company
        gc.collect()
    
    # Store batch results
    ti.xcom_push(key=f'batch_{batch_num}_results', value=results)
    
    print(f"\n✅ Batch {batch_num} complete: {len(results)} companies processed", flush=True)
    print(f"{'='*80}", flush=True)


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
        
        print(f"\n[{i}/{len(companies)}] {company_name}", flush=True)
        
        try:
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


def extract_structured_data(**context):
    """Task 4: Extract structured data with LLM - BATCHED"""
    
    print("\n" + "=" * 80, flush=True)
    print("TASK 4: STRUCTURED EXTRACTION (BATCHED)", flush=True)
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
            
            print(f"\n📦 Processing batch {batch_start//batch_size + 1}: Companies {batch_start+1}-{batch_end}", flush=True)
            
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
            
            # Longer delay between batches
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


def upload_to_gcs(**context):
    """Task 5: Upload structured data to GCS bucket"""
    
    print("\n" + "=" * 80, flush=True)
    print("TASK 5: UPLOADING TO GCS", flush=True)
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
        print(f"✅ UPLOAD COMPLETE", flush=True)
        print(f"   Uploaded: {uploaded} files", flush=True)
        print(f"   Errors: {errors}", flush=True)
        print(f"   Bucket: gs://{bucket_name}/structured/", flush=True)
        print("=" * 80, flush=True)
        
        # Clean up /tmp
        import shutil
        shutil.rmtree("/tmp/data", ignore_errors=True)
        print("🧹 Cleaned up /tmp/data", flush=True)
    
    except Exception as e:
        print(f"❌ Upload error: {e}", flush=True)
        raise


def update_pinecone(**context):
    """Task 6: Update Pinecone vector database"""
    
    print("\n" + "=" * 80, flush=True)
    print("TASK 6: UPDATING PINECONE VECTOR DB", flush=True)
    print("=" * 80, flush=True)
    
    try:
        builder = PineconeVectorDB()
        builder.build_for_all_companies()
        
        print("=" * 80, flush=True)
        print("✅ PINECONE UPDATE COMPLETE", flush=True)
        print("=" * 80, flush=True)
    
    except Exception as e:
        print(f"❌ Pinecone update error: {e}", flush=True)
        import traceback
        traceback.print_exc()
        raise


# ============================================================================
# DEFINE THE DAG WITH PARALLEL BATCHES
# ============================================================================

with DAG(
    dag_id='ai50_full_ingest',
    default_args=default_args,
    description='Full ingestion of Forbes AI 50: Websites + News + APIs → GCS + Pinecone',
    schedule_interval='@once',
    start_date=days_ago(1),
    catchup=False,
    tags=['ai50', 'production', 'full-load', 'batched'],
    doc_md="""
    ## PE Dashboard - Full Ingestion Pipeline (Batched)
    
    Processes companies in 6 parallel batches for reliability and speed.
    
    **Batch Strategy:**
    - Batch 1-6: Process ~9 companies each in parallel
    - Reduces memory pressure per worker
    - Faster completion via parallelism
    - More resilient to failures
    
    **Pipeline Steps:**
    1. Load company list
    2. Scrape websites (6 parallel batches)
    3. Scrape news + APIs
    4. Extract structured data (batched with rate limiting)
    5. Upload to GCS
    6. Update Pinecone
    
    **Runtime:** ~1-2 hours (parallelized)
    **Cost:** ~$10-15 per run
    """,
) as dag:
    
    # ========================================
    # TASK 1: Load Companies
    # ========================================
    task_load = PythonOperator(
        task_id='load_companies',
        python_callable=load_companies,
    )
    
    # ========================================
    # TASK 2: Scrape Websites (6 PARALLEL BATCHES)
    # ========================================
    batch_size = 9  # ~52 companies / 6 batches = ~9 per batch
    
    # Create 6 batch tasks
    scraping_batches = []
    for batch_num in range(1, 7):  # Batches 1-6
        task = PythonOperator(
            task_id=f'scrape_batch_{batch_num}',
            python_callable=scrape_batch,
            params={'batch_num': batch_num, 'batch_size': batch_size},
            execution_timeout=timedelta(hours=2),  # 2 hours per batch
        )
        scraping_batches.append(task)
    
    # ========================================
    # TASK 3: Scrape News + APIs
    # ========================================
    task_scrape_news = PythonOperator(
        task_id='scrape_news_and_apis',
        python_callable=scrape_news_and_apis,
        execution_timeout=timedelta(hours=2),
    )
    
    # ========================================
    # TASK 4: Extract Structured Data
    # ========================================
    task_extract = PythonOperator(
        task_id='extract_structured_data',
        python_callable=extract_structured_data,
        execution_timeout=timedelta(hours=3),
        retries=0,
    )
    
    # ========================================
    # TASK 5: Upload to GCS
    # ========================================
    task_upload = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
    )
    
    # ========================================
    # TASK 6: Update Pinecone
    # ========================================
    task_pinecone = PythonOperator(
        task_id='update_pinecone',
        python_callable=update_pinecone,
        execution_timeout=timedelta(hours=1),
    )

    
    
    # ========================================
    # DEFINE DEPENDENCIES
    # ========================================
    # Load → All 6 batches run in parallel → News → Extract → Upload → Pinecone
    task_load >> scraping_batches >> task_scrape_news >> task_extract >> task_upload >> task_pinecone