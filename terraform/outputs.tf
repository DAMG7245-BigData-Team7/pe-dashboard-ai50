output "fastapi_url" {
  description = "URL of FastAPI service"
  value       = google_cloud_run_v2_service.fastapi.uri
}

output "streamlit_url" {
  description = "URL of Streamlit UI"
  value       = google_cloud_run_v2_service.streamlit.uri
}

output "project_id" {
  description = "GCP Project ID"
  value       = var.project_id
}

output "region" {
  description = "Deployment region"
  value       = var.region
}

# ============================================================================
# COMPOSER OUTPUTS
# ============================================================================

output "composer_environment_name" {
  description = "Cloud Composer environment name"
  value       = google_composer_environment.airflow.name
}

output "composer_airflow_uri" {
  description = "Airflow web UI URL"
  value       = google_composer_environment.airflow.config[0].airflow_uri
}

output "composer_gcs_bucket" {
  description = "Composer DAGs bucket"
  value       = google_composer_environment.airflow.config[0].dag_gcs_prefix
}

output "composer_service_account" {
  description = "Composer service account email"
  value       = google_service_account.composer_sa.email
}