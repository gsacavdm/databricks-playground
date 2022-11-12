resource "databricks_job" "feature_store_nyc_taxi" {
  task {
    task_key = "Feature_Store_-_NYC_Taxi"
    notebook_task {
      notebook_path = "Features/Feature Store (NYC Taxi)"
      base_parameters = {
        notebook_mode        = "Recurring"
      }
    }
    new_cluster {
      spark_version = "11.3.x-cpu-ml-scala2.12"
      node_type_id = "r3.xlarge"
      spark_conf = {
        "spark.databricks.delta.preview.enabled" = "true"
      }
      runtime_engine     = "STANDARD"
      num_workers        = 1
      data_security_mode = "SINGLE_USER"
    }
    email_notifications {
    }
  }
  schedule {
    timezone_id            = "America/Los_Angeles"
    quartz_cron_expression = "36 0 6 * * ?"
  }
  name                = "Feature Store - NYC Taxi"
  max_concurrent_runs = 1
  git_source {
    url      = "https://github.com/gsacavdm/databricks-playground"
    provider = "gitHub"
    branch   = "master"
  }
}
