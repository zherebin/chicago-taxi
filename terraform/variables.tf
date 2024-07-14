variable "project" {
  description = "Project"
  # Change the project id to the one you've created
  default    = "vertical-orbit-426819-f8"
}

variable "region" {
  description = "Region"
  # Feel free to change the region
  default    = "us-central1"
}

variable "location" {
  description = "Project Location"
  default    = "US"
}

variable "zone" {
  description = "Zone"
  # Feel free to change the zone
  default     = "us-central1-c"
}

variable "gcs_bucket_name" {
  description = "Storage bucket name"
  # Feel free to change the bucket name
  default     = "vertical-orbit-426819-f8-terra-bucket"
}

variable "gcs_bucket_storage_class" {
  description = "Storage bucket class"
  default     = "STANDARD"
}

variable "bq_dataset_name" {
  description = "BigQuery dataset name"
  # Feel free to change the dataset name
  default     = "trips_data"
}