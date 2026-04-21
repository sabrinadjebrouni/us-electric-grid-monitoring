variable "credentials" {
  description = "GCS credentials"
  default     = ""
}

variable "project_name" {
  description = "Project Name"
  default     = ""
}

variable "region" {
  description = "Region of US"
  default     = ""
}


variable "location" {
  description = "Project location"
  default     = ""
}

variable "bq_dataset_name" {
  description = "BigQuery Dataset Name"
  default     = ""
}

variable "gcs_bucket_name" {
  description = "Bucket Storage Name"
  default     = ""
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}