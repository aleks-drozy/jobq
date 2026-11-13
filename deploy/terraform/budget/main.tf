# Apply this before the cluster, and leave it applied.
#
# It creates one thing: a monthly cost budget for the whole account that
# emails you at 50%, 80% and 100% of the limit, and once more if the month
# is forecast to exceed it. It costs nothing (the first two budgets in an
# account are free) and it is the only protection against forgetting to
# run `terraform destroy` on the cluster.
#
# A budget alerts; it does not stop anything. Billing data lags by hours.

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  # Budgets is a global service served from us-east-1.
  region = "us-east-1"
}

variable "monthly_limit_usd" {
  description = "Monthly spend, in US dollars, to be warned about."
  type        = number
  default     = 10
}

variable "email" {
  description = "Where the warnings go."
  type        = string
}

resource "aws_budgets_budget" "monthly" {
  name         = "jobq-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_limit_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  dynamic "notification" {
    for_each = [50, 80, 100]
    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value
      threshold_type             = "PERCENTAGE"
      notification_type          = "ACTUAL"
      subscriber_email_addresses = [var.email]
    }
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.email]
  }
}
