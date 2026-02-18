variable "project_id" {
  description = "The GCP project ID"
  default     = "plated-monolith-486816-j0"
}

variable "location" {
  description = "The location of the GCP resources"
  default     = "europe-west2"
}

variable "bq_dataset_name" {
  description = "The name of the BigQuery dataset"
  default     = "zoomcamp"
}

variable "gcs_bucket_name" {
  description = "The name of the GCS bucket (must be globally unique)"
  default     = "plated-monolith-486816-j0-zoomcamp"
}

variable "credentials_file" {
  description = "The path to the service account credentials JSON file"
  default     = "keys/my-creds.json"
}
