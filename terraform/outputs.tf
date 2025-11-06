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