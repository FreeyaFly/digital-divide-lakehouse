data "azurerm_client_config" "current" {}

resource "random_string" "suffix" {
  length  = 5
  upper   = false
  special = false
}

locals {
  name_prefix = "${var.project}-${var.environment}"
  # Storage account and Key Vault names are global: alphanumeric, <= 24 chars.
  compact_name = "${var.project}${var.environment}${random_string.suffix.result}"

  tags = {
    project     = var.project
    environment = var.environment
    managed_by  = "terraform"
  }

  lake_containers = ["landing", "bronze", "silver", "gold"]
}

resource "azurerm_resource_group" "main" {
  name     = "rg-${local.name_prefix}"
  location = var.location
  tags     = local.tags
}

# --- Data lake -------------------------------------------------------------

resource "azurerm_storage_account" "lake" {
  name                            = "st${local.compact_name}"
  resource_group_name             = azurerm_resource_group.main.name
  location                        = azurerm_resource_group.main.location
  account_kind                    = "StorageV2"
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  is_hns_enabled                  = true # ADLS Gen2
  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false
  tags                            = local.tags
}

resource "azurerm_storage_container" "lake" {
  for_each              = toset(local.lake_containers)
  name                  = each.value
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

# Raw landing files are replayable from source, so age them out cheaply.
resource "azurerm_storage_management_policy" "lake" {
  storage_account_id = azurerm_storage_account.lake.id

  rule {
    name    = "landing-tiering"
    enabled = true

    filters {
      blob_types   = ["blockBlob"]
      prefix_match = ["landing/"]
    }

    actions {
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than = 30
        delete_after_days_since_modification_greater_than       = 180
      }
    }
  }
}

resource "azurerm_role_assignment" "me_lake" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
}

# --- Pipeline identity -------------------------------------------------------
# Service principal for the orchestrated pipeline (Airflow task containers): it lands
# World Bank files in the landing container and signs short-lived user delegation SAS
# for Databricks. Its client secret is created outside Terraform
# (az ad app credential reset) so it never lands in the state file.

resource "azuread_application" "pipeline" {
  display_name = "sp-${local.name_prefix}-pipeline"
  owners       = [data.azurerm_client_config.current.object_id]
}

resource "azuread_service_principal" "pipeline" {
  client_id = azuread_application.pipeline.client_id
  owners    = [data.azurerm_client_config.current.object_id]
}

# Write access to the landing container only, not to the other lake containers.
resource "azurerm_role_assignment" "pipeline_landing" {
  scope                = azurerm_storage_container.lake["landing"].id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azuread_service_principal.pipeline.object_id
}

# Requesting a user delegation key is an account-level action; the SAS it signs is still
# limited to what the principal can do (the landing container) and to the SAS scope.
resource "azurerm_role_assignment" "pipeline_delegator" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Delegator"
  principal_id         = azuread_service_principal.pipeline.object_id
}

# --- Secrets ---------------------------------------------------------------

resource "azurerm_key_vault" "main" {
  name                       = "kv-${local.compact_name}"
  resource_group_name        = azurerm_resource_group.main.name
  location                   = azurerm_resource_group.main.location
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  rbac_authorization_enabled = true
  soft_delete_retention_days = 7
  purge_protection_enabled   = false
  tags                       = local.tags
}

resource "azurerm_role_assignment" "me_kv" {
  scope                = azurerm_key_vault.main.id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = data.azurerm_client_config.current.object_id
}

# --- Cost guardrails ---------------------------------------------------------
# Hard cap for the whole project: €1. Azure budgets only alert and never stop
# spending, so the real guardrail is the architecture: storage and Key Vault
# only, lake data kept under 1 GB, heavy compute on Databricks Free Edition.

# Defender for Storage is billed per account; pin the subscription plan to Free.
resource "azurerm_security_center_subscription_pricing" "storage" {
  tier          = "Free"
  resource_type = "StorageAccounts"
}

# Subscription scope, not resource group: the subscription holds only this project, so the
# budget also covers what Terraform does not manage, such as the remote-state account
# (infra/bootstrap) and anything created elsewhere by mistake.
resource "azurerm_consumption_budget_subscription" "main" {
  name            = "budget-${local.name_prefix}"
  subscription_id = "/subscriptions/${var.subscription_id}"
  amount          = var.budget_amount
  time_grain      = "Monthly"

  time_period {
    start_date = formatdate("YYYY-MM-01'T'00:00:00Z", timestamp())
  }

  dynamic "notification" {
    for_each = {
      actual_20    = { threshold = 20, type = "Actual" }
      actual_50    = { threshold = 50, type = "Actual" }
      actual_100   = { threshold = 100, type = "Actual" }
      forecast_100 = { threshold = 100, type = "Forecasted" }
    }
    content {
      enabled        = true
      threshold      = notification.value.threshold
      threshold_type = notification.value.type
      operator       = "GreaterThan"
      contact_emails = var.budget_contact_emails
    }
  }

  lifecycle {
    # start_date is derived from timestamp(); pin it after first apply.
    ignore_changes = [time_period]
  }
}
