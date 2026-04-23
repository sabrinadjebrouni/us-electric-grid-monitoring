variable "credentials" {
  description = "GCS credentials"
}

variable "project_name" {
  description = "Project Name, is the same as the project id found in GCP account"
}

variable "region" {
  description = "Region where to create the bucket/dataset/VM for example us-central1"
}


variable "location" {
  description = "Project location for example US, europe ...ect"
}

variable "bq_dataset_name" {
  description = "BigQuery Dataset Name"
}

variable "gcs_bucket_name" {
  description = "Bucket Storage Name, default is project-id-bucket"
}

variable "gcs_scripts_bucket_name" {
  description = "Bucket for scripts like spark jobs"
  
}
variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "dataproc_cluster_name" {
  description = "Dataproc Cluster Name"
}