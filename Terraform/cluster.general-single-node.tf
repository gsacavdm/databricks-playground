resource "databricks_cluster" "gen_single_node_cluster" {
  spark_version = "11.3.x-scala2.12"
  node_type_id = "r3.xlarge"
  spark_conf = {
    "spark.databricks.cluster.profile" = "singleNode"
    "spark.master"                     = "local[*, 4]"
  }
  runtime_engine = "STANDARD"
  custom_tags = {
    ResourceClass = "SingleNode"
  }
  cluster_name = "General Single Node Cluster"
  aws_attributes {
    zone_id                = "us-west-2c"
    spot_bid_price_percent = 100
    first_on_demand        = 1
    availability           = "SPOT_WITH_FALLBACK"
  }
  autotermination_minutes = 120
}
