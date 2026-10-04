variable "subscription_id" {
  description = "Azure subscription ID (az account show --query id -o tsv)."
  type        = string
}

variable "project" {
  description = "Short project name used in resource names."
  type        = string
  default     = "ddlake"
}

variable "environment" {
  description = "Deployment environment."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment must be dev or prod."
  }
}

variable "location" {
  description = "Azure region. West Europe rejects new customers for storage/Key Vault, so use its EU pair North Europe."
  type        = string
  default     = "northeurope"
}

variable "budget_amount" {
  description = "Monthly budget for the resource group, in the billing currency. The whole project is capped at €1."
  type        = number
  default     = 1
}

variable "budget_contact_emails" {
  description = "Addresses that receive budget alerts."
  type        = list(string)
}
