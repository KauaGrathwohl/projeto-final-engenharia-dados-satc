variable "resource_group_name" {
  description = "Nome do Resource Group do Databricks"
  type        = string
  default     = "rg-iac-databricks"
}

variable "location" {
  description = "Localização do Databricks"
  type        = string
  default     = "East US"
}

variable "subscription_id" {
  description = "ID da Subscription na Azure"
  type        = string
}