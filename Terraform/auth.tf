variable "databricks_connection_profile" {}

terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

provider "databricks" {
  profile = var.databricks_connection_profile
}
