output "resource_group_name" {
  value = azurerm_resource_group.main.name
}

output "storage_account_name" {
  value = azurerm_storage_account.lake.name
}

output "lake_dfs_endpoint" {
  value = azurerm_storage_account.lake.primary_dfs_endpoint
}

output "key_vault_uri" {
  value = azurerm_key_vault.main.vault_uri
}

output "pipeline_client_id" {
  description = "Application (client) ID of the pipeline service principal."
  value       = azuread_application.pipeline.client_id
}

output "tenant_id" {
  value = data.azurerm_client_config.current.tenant_id
}
