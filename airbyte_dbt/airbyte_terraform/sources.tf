data "airbyte_connector_configuration" "postgres_config" {
  connector_name = "source-postgres"

  configuration = {
    host               = "host.docker.internal"
    port               = 5432
    database           = "postgres"
    username           = "your_read_only_user"
    schemas            = ["public"]
    ssl_mode           = { mode = "disable" }
    replication_method = { method = "Xmin" }
  }

  configuration_secrets = {
    password = var.postgres_password
  }
}

resource "airbyte_source" "postgres" {
  name          = "postgres_source"
  workspace_id  = var.workspace_id
  definition_id = data.airbyte_connector_configuration.postgres_config.definition_id
  configuration = data.airbyte_connector_configuration.postgres_config.configuration_json
}