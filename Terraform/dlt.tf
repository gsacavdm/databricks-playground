resource "databricks_pipeline" "dlt_demo" {
  target  = "default"
  name    = "DLT Demo"
  library {
    notebook {
      path = "/Repos/${data.databricks_current_user.me.user_name}/databricks-playground/Features/DLT"
    }
  }
  edition = "ADVANCED"
  cluster {
    label = "default"
    autoscale {
      mode        = "ENHANCED"
      min_workers = 1
      max_workers = 5
    }
  }
  channel = "CURRENT"
}
