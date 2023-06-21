locals {
  common_config = fileexists(var.common_yaml_config_path) ? yamldecode(file(var.common_yaml_config_path)) : object({})
}

module "cloud_storage" {
  source  = "gitlab.agile.nat.bt.com/CDHS/cloud-storage/google"
  version = "0.0.9"

  project_id           = local.common_config.data_project_id
  stage                = local.common_config.stage
  gcs_yaml_config_path = var.common_yaml_config_path
}

module "big-query" {
  source = "gitlab.agile.nat.bt.com/CDHS/big-query/google"
  version = "0.0.16"

  project_id           = local.common_config.data_project_id
  stage                = local.common_config.stage
  bq_yaml_config_path = var.common_yaml_config_path
}

module "iam-access" {
  source  = "gitlab.agile.nat.bt.com/CDHS/cdh-project-sa/google"
  version = "0.0.1"
  
  sa_yaml_config_path = var.common_yaml_config_path
  stage               = local.common_config.stage
  project_id          = local.common_config.data_project_id
}

module "project-log-sink" {
  source = "gitlab.agile.nat.bt.com/CDHS/cdh-project-log-sink/google"
  version = "0.0.4"

} 

module "service_accounts" {
  source              = "gitlab.agile.nat.bt.com/CDHS/service-accounts-wrapper/google"
  version             = "0.0.2"
  stage               = local.common_config.stage
  project_id          = local.common_config.data_project_id
  sa_yaml_config_path = var.common_yaml_config_path
}

module "monitoring" {
  source = "gitlab.agile.nat.bt.com/CDHS/monitoring/google"
  version = "0.0.8"

  monitoring_yaml_config_path = var.common_yaml_config_path
}