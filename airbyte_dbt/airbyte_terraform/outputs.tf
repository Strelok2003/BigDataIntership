output "airbyte_connection_id" {
  description = "The ID of the newly created Airbyte connection"
  value       = airbyte_connection.postgres_to_snowflake.connection_id
}