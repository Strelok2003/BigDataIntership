resource "airbyte_connection" "postgres_to_snowflake" {
  name           = "Postgres to Snowflake"
  source_id      = airbyte_source.postgres.source_id
  destination_id = airbyte_destination.snowflake.destination_id

  status = "active"

  schedule = {
    schedule_type = "manual"
  }

  namespace_definition = "destination"
  prefix               = ""

  configurations = {
    streams = [
      {
        name         = "actor"
        namespace    = "public"
        sync_mode    = "incremental_deduped_history"
        cursor_field = []
        primary_key  = [["actor_id"]]
      },
      {
        name         = "address"
        namespace    = "public"
        sync_mode    = "incremental_deduped_history"
        cursor_field = []
        primary_key  = [["address_id"]]
      },
      {
        name         = "category"
        namespace    = "public"
        sync_mode    = "incremental_deduped_history"
        cursor_field = []
        primary_key  = [["category_id"]]
      },
      {
        name         = "city"
        namespace    = "public"
        sync_mode    = "incremental_deduped_history"
        cursor_field = []
        primary_key  = [["city_id"]]
      },
      {
        name         = "country"
        namespace    = "public"
        sync_mode    = "incremental_deduped_history"
        cursor_field = []
        primary_key  = [["country_id"]]
      },
      {
        name         = "customer"
        namespace    = "public"
        sync_mode    = "incremental_deduped_history"
        cursor_field = []
        primary_key  = [["customer_id"]]
      },
      {
        name         = "film"
        namespace    = "public"
        sync_mode    = "incremental_deduped_history"
        cursor_field = []
        primary_key  = [["film_id"]]
      },
      {
        name         = "film_actor"
        namespace    = "public"
        sync_mode    = "incremental_deduped_history"
        cursor_field = []
        primary_key  = [["actor_id"], ["film_id"]]
      },
      {
        name         = "film_category"
        namespace    = "public"
        sync_mode    = "incremental_deduped_history"
        cursor_field = []
        primary_key  = [["film_id"], ["category_id"]]
      },
      {
        name         = "inventory"
        namespace    = "public"
        sync_mode    = "incremental_deduped_history"
        cursor_field = []
        primary_key  = [["inventory_id"]]
      },
      {
        name         = "language"
        namespace    = "public"
        sync_mode    = "incremental_deduped_history"
        cursor_field = []
        primary_key  = [["language_id"]]
      },
      {
        name         = "payment"
        namespace    = "public"
        sync_mode    = "incremental_deduped_history"
        cursor_field = []
        primary_key  = [["payment_date"], ["payment_id"]]
      },
      {
        name         = "rental"
        namespace    = "public"
        sync_mode    = "incremental_deduped_history"
        cursor_field = []
        primary_key  = [["rental_id"]]
      },
      {
        name         = "staff"
        namespace    = "public"
        sync_mode    = "incremental_deduped_history"
        cursor_field = []
        primary_key  = [["staff_id"]]
      },
      {
        name         = "store"
        namespace    = "public"
        sync_mode    = "incremental_deduped_history"
        cursor_field = []
        primary_key  = [["store_id"]]
      }
    ]
  }
}