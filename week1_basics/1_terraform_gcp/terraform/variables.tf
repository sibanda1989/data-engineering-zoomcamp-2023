locals {
  data_lake_bucket = "dtc_data_lake"
}

variable "project" {
  description = "dtc-de-course-389809"
}

variable "region" {
  description = "europe-west6"
  default = "europe-west6"
  type = string
}

variable "storage_class" {
  description = "STANDARD"
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "trips_data_all"
}