# ============================================================================
# CLOUD COMPOSER 2.x (Airflow 2)
# ============================================================================

# Grant Composer Service Agent the required role
resource "google_project_iam_member" "composer_service_agent" {
  project = var.project_id
  role    = "roles/composer.ServiceAgentV2Ext"
  member  = "serviceAccount:service-${data.google_project.project.number}@cloudcomposer-accounts.iam.gserviceaccount.com"
  
  depends_on = [google_project_service.required_apis]
}

# Create dedicated service account for Composer workers
resource "google_service_account" "composer_sa" {
  account_id   = "composer-airflow-sa"
  display_name = "Cloud Composer Service Account"
  description  = "Service account for Cloud Composer Airflow workers"
  
  depends_on = [google_project_iam_member.composer_service_agent]
}

# Grant necessary roles to Composer service account
resource "google_project_iam_member" "composer_worker" {
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
  
  depends_on = [google_service_account.composer_sa]
}

resource "google_project_iam_member" "composer_storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
  
  depends_on = [google_service_account.composer_sa]
}

resource "google_project_iam_member" "composer_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
  
  depends_on = [google_service_account.composer_sa]
}

# Cloud Composer Environment
resource "google_composer_environment" "airflow" {
  name   = "pe-dashboard-airflow"
  region = var.region
  
  config {
    # Software configuration
    software_config {
      image_version = "composer-2-airflow-2"
      
      # ========================================
      # AIRFLOW CONFIGURATION OVERRIDES
      # Enable parallel task execution
      # ========================================
      airflow_config_overrides = {
        core-parallelism                 = "32"   # Max tasks across all DAGs
        core-dag_concurrency             = "16"   # Max concurrent tasks per DAG
        core-max_active_tasks_per_dag    = "16"   # Max active tasks per DAG
        core-max_active_runs_per_dag     = "1"    # Only 1 DAG run at a time
        scheduler-max_tis_per_query      = "16"   # Scheduler query efficiency
        #celery-worker_autoscale          = "8,2"  # Workers scale from 2 to 8
      }
      
      # Python packages
      pypi_packages = {
        openai               = ">=1.3.0"
        instructor           = ">=0.4.0"
        pinecone             = ">=3.0.0"  # Fixed from pinecone-client
        google-cloud-storage = ">=2.10.0"
        requests             = ">=2.31.0"
        beautifulsoup4       = ">=4.12.0"
        lxml                 = ">=4.9.0"
        fake-useragent       = ">=1.4.0"
        playwright           = ">=1.40.0"
        selenium             = ">=4.15.0"
        pandas               = ">=2.1.0"
        pydantic             = ">=2.0.0"
        tiktoken             = ">=0.5.0"
        python-dotenv        = ">=1.0.0"
      }
      
      # Environment variables
      env_variables = {
        OPENAI_API_KEY   = var.openai_api_key
        PINECONE_API_KEY = var.pinecone_api_key
        DATA_BUCKET      = google_storage_bucket.data_bucket.name
      }
    }
    
    # Workloads configuration (Increased resources)
    workloads_config {
      scheduler {
        cpu        = 1        # Increased from 0.5
        memory_gb  = 3.75     # Increased from 1.875
        storage_gb = 2        # Increased from 1
        count      = 1
      }
      
      web_server {
        cpu        = 1        # Increased from 0.5
        memory_gb  = 3.75     # Increased from 1.875
        storage_gb = 2        # Increased from 1
      }
      
      worker {
        cpu        = 2        # Increased from 0.5 (4x)
        memory_gb  = 7.5      # Increased from 1.875 (4x)
        storage_gb = 5        # Increased from 1 (5x)
        min_count  = 2        # Start with 2 workers (was 1)
        max_count  = 6        # Scale up to 6 workers (was 3)
      }
    }
    
    # Environment size (Upgraded to MEDIUM)
    environment_size = "ENVIRONMENT_SIZE_MEDIUM"
    
    # Node configuration with service account
    node_config {
      service_account = google_service_account.composer_sa.email
    }
  }
  
  depends_on = [
    google_project_service.required_apis,
    google_storage_bucket.data_bucket,
    google_secret_manager_secret_version.openai_api_key_version,
    google_secret_manager_secret_version.pinecone_api_key_version,
    google_service_account.composer_sa,
    google_project_iam_member.composer_worker,
    google_project_iam_member.composer_storage_admin,
    google_project_iam_member.composer_secret_accessor,
    google_project_iam_member.composer_service_agent
  ]
}

# Grant Composer service account access to data bucket
resource "google_storage_bucket_iam_member" "composer_bucket_access" {
  bucket = google_storage_bucket.data_bucket.name
  role   = "roles/storage.admin"
  member = "serviceAccount:${google_service_account.composer_sa.email}"
  
  depends_on = [google_composer_environment.airflow]
}

# Grant Composer service account access to Secret Manager
resource "google_secret_manager_secret_iam_member" "composer_openai_access" {
  secret_id = google_secret_manager_secret.openai_api_key.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.composer_sa.email}"
  
  depends_on = [google_composer_environment.airflow]
}

resource "google_secret_manager_secret_iam_member" "composer_pinecone_access" {
  secret_id = google_secret_manager_secret.pinecone_api_key.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.composer_sa.email}"
  
  depends_on = [google_composer_environment.airflow]
}