terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
  zone    = var.zone
}

resource "google_storage_bucket" "trips-bucket" {
  name     = var.gcs_bucket_name
  location = var.location

  storage_class = var.gcs_bucket_storage_class
  force_destroy = true
}

resource "google_bigquery_dataset" "trips_data" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}
