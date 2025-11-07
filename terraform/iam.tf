# ============================================================================
# GITHUB ACTIONS SERVICE ACCOUNT PERMISSIONS
# ============================================================================

# Grant GitHub Actions SA access to Composer
resource "google_project_iam_member" "github_actions_composer_viewer" {
  project = var.project_id
  role    = "roles/composer.user"
  member  = "serviceAccount:github-actions-sa@${var.project_id}.iam.gserviceaccount.com"
}

# Grant GitHub Actions SA access to Cloud Run (for deployments)
resource "google_project_iam_member" "github_actions_run_admin" {
  project = var.project_id
  role    = "roles/run.admin"
  member  = "serviceAccount:github-actions-sa@${var.project_id}.iam.gserviceaccount.com"
}

# Grant GitHub Actions SA access to Artifact Registry
resource "google_project_iam_member" "github_actions_artifact_writer" {
  project = var.project_id
  role    = "roles/artifactregistry.writer"
  member  = "serviceAccount:github-actions-sa@${var.project_id}.iam.gserviceaccount.com"
}

# Grant GitHub Actions SA access to Secret Manager
resource "google_project_iam_member" "github_actions_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:github-actions-sa@${var.project_id}.iam.gserviceaccount.com"
}

# Grant GitHub Actions SA access to Storage
resource "google_project_iam_member" "github_actions_storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:github-actions-sa@${var.project_id}.iam.gserviceaccount.com"
}