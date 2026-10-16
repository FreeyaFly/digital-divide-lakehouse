terraform {
  required_version = ">= 1.6"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 3.10"
    }
  }

  # Remote state in a storage account outside this configuration (infra/bootstrap/
  # create_tfstate.ps1). Its names are not in the repository: they are passed with
  # `terraform init -backend-config=backend.hcl` (see backend.hcl.example).
  backend "azurerm" {
    use_azuread_auth = true # Entra ID data-plane auth: the state account has shared keys off
  }
}

provider "azurerm" {
  subscription_id = var.subscription_id

  features {
    key_vault {
      purge_soft_delete_on_destroy = true
    }
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }
}

# Same signed-in tenant as azurerm; used for the pipeline's service principal.
provider "azuread" {}
