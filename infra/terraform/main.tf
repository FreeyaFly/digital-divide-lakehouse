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

resource "azurerm_consumption_budget_resource_group" "main" {
  name              = "budget-${local.name_prefix}"
  resource_group_id = azurerm_resource_group.main.id
  amount            = var.budget_amount
  time_grain        = "Monthly"

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
