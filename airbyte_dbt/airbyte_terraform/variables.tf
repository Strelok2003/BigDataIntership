# variable "client_id" {
#   type      = string
#   sensitive = true
# }

# variable "client_secret" {
#   type      = string
#   sensitive = true
# }

variable "airbyte_username" {
  type      = string
  sensitive = true

}

variable "airbyte_password" {
  type      = string
  sensitive = true

}

variable "workspace_id" {
  type = string
}

variable "postgres_password" {
  type      = string
  sensitive = true
}

variable "aribyte_server_url" {
  type = string
}

variable "snowflake_host" {
  type      = string
  sensitive = true
}

variable "snowflake_password" {
  type      = string
  sensitive = true
}