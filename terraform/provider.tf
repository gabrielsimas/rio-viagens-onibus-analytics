terraform {
  required_providers {
    google = {
        source = "hashicorp/google"
        version = ">= 4.0"
    }
  }

  backend "gcs" {
    bucket = "mvp-rio-transportes-tf-state"
    prefix = "terraform/state"
  }
}

provider "google" {
  project = var.project_id
  region = var.region
}