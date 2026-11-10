# Optional budget: alerts on real cost before credits; does not block spending.
resource "aws_budgets_budget" "data_platform" {
  count        = var.enable_budget ? 1 : 0
  name         = "${var.name_prefix}-data-platform-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.budget_amount_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  # Credits not deducted: shows the real amount being burned, like EXCLUDE_ALL_CREDITS on GCP.
  cost_types {
    include_credit = false
  }

  dynamic "notification" {
    for_each = [50, 90, 100]
    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value
      threshold_type             = "PERCENTAGE"
      notification_type          = "ACTUAL"
      subscriber_email_addresses = [var.budget_email]
    }
  }

  lifecycle {
    precondition {
      condition     = var.budget_email != null
      error_message = "enable_budget=true requires budget_email; AWS does not send alerts to anyone on its own."
    }
  }
}
