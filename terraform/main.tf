terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project_name
}

resource "google_storage_bucket" "raw_data" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true
} /*force_destroy - (Optional, Default: false) When deleting a bucket,
  this boolean option will delete all contained objects. 
  If you try to delete a bucket that contains objects, Terraform will fail that run.*/

resource "google_bigquery_dataset" "dataset" {
  dataset_id                 = var.bq_dataset_name
  location                   = var.location
  delete_contents_on_destroy = true
} /*same for delete_contents_on_destroy when destroying a dataset*/


resource "google_dataproc_cluster" "dataproc_cluster" {
  name     = "eia-spark-cluster"
  region   = "us-central1"

  cluster_config {

    cluster_tier = "CLUSTER_TIER_STANDARD"

    software_config {
      optional_components = ["JUPYTER"]
    }

    master_config { 
      num_instances = 1
      machine_type  = "n1-standard-4" 
      disk_config {
        boot_disk_type    = "pd-ssd" #Persistent Disk SSD
        boot_disk_size_gb = 30 #set disk size to 30GB, default value is 500GB
      }
    }

    worker_config {
      num_instances    = 2
      machine_type     = "n1-standard-8" 
      disk_config {
        boot_disk_size_gb = 30  #set disk size to 30GB, default value is 500GB
      }
    }

  }
} 