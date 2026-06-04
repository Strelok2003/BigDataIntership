data "airbyte_connector_configuration" "snowflake_config" {
  connector_name = "destination-snowflake"

  configuration = {
    host              = var.snowflake_host
    role              = "AIRBYTE_ROLE"
    schema            = "AIRBYTE_SCHEMA"
    database          = "AIRBYTE_DATABASE"
    username          = "AIRBYTE_USER"
    warehouse         = "AIRBYTE_WAREHOUSE"
    trim_space        = true
    cdc_deletion_mode = "Hard delete"
  }

  configuration_secrets = {
    credentials = {
      password  = var.snowflake_password
      auth_type = "Username and Password"
    }
  }
}

resource "airbyte_destination" "snowflake" {
  name          = "snowflake_destination"
  workspace_id  = var.workspace_id
  definition_id = data.airbyte_connector_configuration.snowflake_config.definition_id
  configuration = data.airbyte_connector_configuration.snowflake_config.configuration_json
}