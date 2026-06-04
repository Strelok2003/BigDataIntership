terraform {
  required_providers {
    airbyte = {
      source  = "airbytehq/airbyte"
      version = "1.2.0"
    }
  }
}

provider "airbyte" {
  server_url = var.aribyte_server_url
  username   = var.airbyte_username
  password   = var.airbyte_password

}