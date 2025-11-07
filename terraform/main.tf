# Terraform Configuration for GCP Cloud Run Deployment

terraform {
  required_version = ">= 1.0"
  
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
  
  # Store state in GCS (after initial setup)
  # backend "gcs" {
  #   bucket = "pe-dashboard-terraform-state"
  #   prefix = "terraform/state"
  # }
}

# Configure GCP Provider
provider "google" {
  project = var.project_id
  region  = var.region
}

# Enable required APIs
resource "google_project_service" "required_apis" {
  for_each = toset([
    "run.googleapis.com",
    "containerregistry.googleapis.com",
    "cloudbuild.googleapis.com",
    "secretmanager.googleapis.com",
    "storage.googleapis.com"  # Added for GCS
  ])
  
  service            = each.key
  disable_on_destroy = false
}

# Get project data
data "google_project" "project" {
  project_id = var.project_id
}

# ============================================================================
# GCS BUCKET FOR DATA STORAGE
# ============================================================================

# GCS Bucket for storing company data
resource "google_storage_bucket" "data_bucket" {
  name     = "${var.project_id}-pe-dashboard-data"
  location = var.region
  
  # Uniform access control (recommended)
  uniform_bucket_level_access = true
  
  # Enable versioning for data safety
  versioning {
    enabled = true
  }
  
  # Lifecycle rules
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age        = 90  # Delete archived files older than 90 days
      with_state = "ARCHIVED"
    }
  }
  
  lifecycle_rule {
    action {
      type = "SetStorageClass"
      storage_class = "NEARLINE"
    }
    condition {
      age = 30  # Move to cheaper storage after 30 days
    }
  }
  
  # Labels for organization
  labels = {
    environment = "production"
    project     = "pe-dashboard"
    managed_by  = "terraform"
  }
  
  depends_on = [google_project_service.required_apis]
}

# Grant Cloud Run service account READ access to bucket
resource "google_storage_bucket_iam_member" "cloudrun_bucket_reader" {
  bucket = google_storage_bucket.data_bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${data.google_project.project.number}-compute@developer.gserviceaccount.com"
  
  depends_on = [google_storage_bucket.data_bucket]
}

# Grant Cloud Run service account WRITE access (for future Airflow DAG uploads)
resource "google_storage_bucket_iam_member" "cloudrun_bucket_writer" {
  bucket = google_storage_bucket.data_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${data.google_project.project.number}-compute@developer.gserviceaccount.com"
  
  depends_on = [google_storage_bucket.data_bucket]
}

# ============================================================================
# SECRET MANAGER FOR API KEYS
# ============================================================================

# Secret Manager for OpenAI API Key
resource "google_secret_manager_secret" "openai_api_key" {
  secret_id = "openai-api-key"
  
  replication {
    auto {}
  }
  
  depends_on = [google_project_service.required_apis]
}

resource "google_secret_manager_secret_version" "openai_api_key_version" {
  secret      = google_secret_manager_secret.openai_api_key.id
  secret_data = var.openai_api_key
}

# Secret Manager for Pinecone API Key
resource "google_secret_manager_secret" "pinecone_api_key" {
  secret_id = "pinecone-api-key"
  
  replication {
    auto {}
  }
  
  depends_on = [google_project_service.required_apis]
}

resource "google_secret_manager_secret_version" "pinecone_api_key_version" {
  secret      = google_secret_manager_secret.pinecone_api_key.id
  secret_data = var.pinecone_api_key
}

# Grant Secret Manager access to default Cloud Run service account
resource "google_secret_manager_secret_iam_member" "default_sa_openai_access" {
  secret_id = google_secret_manager_secret.openai_api_key.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${data.google_project.project.number}-compute@developer.gserviceaccount.com"
  
  depends_on = [google_secret_manager_secret_version.openai_api_key_version]
}

resource "google_secret_manager_secret_iam_member" "default_sa_pinecone_access" {
  secret_id = google_secret_manager_secret.pinecone_api_key.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${data.google_project.project.number}-compute@developer.gserviceaccount.com"
  
  depends_on = [google_secret_manager_secret_version.pinecone_api_key_version]
}

# ============================================================================
# CLOUD RUN SERVICES
# ============================================================================

# Cloud Run Service - FastAPI
resource "google_cloud_run_v2_service" "fastapi" {
  name     = "pe-dashboard-api"
  location = var.region
  
  template {
    containers {
      image = "${var.region}-docker.pkg.dev/${var.project_id}/pe-dashboard/fastapi:latest"
      
      ports {
        container_port = 8000
      }
      
      # API Keys from Secret Manager
      env {
        name = "OPENAI_API_KEY"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.openai_api_key.secret_id
            version = "latest"
          }
        }
      }
      
      env {
        name = "PINECONE_API_KEY"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.pinecone_api_key.secret_id
            version = "latest"
          }
        }
      }
      
      # GCS Configuration (NEW)
      env {
        name  = "USE_GCS"
        value = "true"
      }
      
      env {
        name  = "GCS_BUCKET"
        value = google_storage_bucket.data_bucket.name
      }
      
      resources {
        limits = {
          cpu    = "2"
          memory = "2Gi"
        }
        cpu_idle = true
      }
    }
    
    scaling {
      min_instance_count = 0
      max_instance_count = 10
    }
  }
  
  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }
  
  depends_on = [
    google_project_service.required_apis,
    google_secret_manager_secret_version.openai_api_key_version,
    google_secret_manager_secret_version.pinecone_api_key_version,
    google_secret_manager_secret_iam_member.default_sa_openai_access,
    google_secret_manager_secret_iam_member.default_sa_pinecone_access,
    google_storage_bucket.data_bucket,
    google_storage_bucket_iam_member.cloudrun_bucket_reader
  ]
}

# Cloud Run Service - Streamlit
resource "google_cloud_run_v2_service" "streamlit" {
  name     = "pe-dashboard-ui"
  location = var.region
  
  template {
    containers {
      image = "${var.region}-docker.pkg.dev/${var.project_id}/pe-dashboard/streamlit:latest"
      
      ports {
        container_port = 8501
      }
      
      env {
        name  = "API_BASE"
        value = google_cloud_run_v2_service.fastapi.uri
      }
      
      resources {
        limits = {
          cpu    = "1"
          memory = "1Gi"
        }
        cpu_idle = true
      }
    }
    
    scaling {
      min_instance_count = 0
      max_instance_count = 5
    }
  }
  
  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }
  
  depends_on = [
    google_project_service.required_apis,
    google_cloud_run_v2_service.fastapi,
    google_secret_manager_secret_iam_member.default_sa_openai_access,
    google_secret_manager_secret_iam_member.default_sa_pinecone_access
  ]
}

# ============================================================================
# IAM - PUBLIC ACCESS (for demo)
# ============================================================================

# Allow public access to FastAPI
resource "google_cloud_run_v2_service_iam_member" "fastapi_public" {
  name     = google_cloud_run_v2_service.fastapi.name
  location = google_cloud_run_v2_service.fastapi.location
  role     = "roles/run.invoker"
  member   = "allUsers"
}

# Allow public access to Streamlit
resource "google_cloud_run_v2_service_iam_member" "streamlit_public" {
  name     = google_cloud_run_v2_service.streamlit.name
  location = google_cloud_run_v2_service.streamlit.location
  role     = "roles/run.invoker"
  member   = "allUsers"
}