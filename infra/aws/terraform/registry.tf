# Cloud registry enabled explicitly; images are kept on destroy and tags are immutable.
resource "aws_ecr_repository" "application" {
  for_each = var.enable_ecr ? toset([
    "platform-generator", "platform-airflow", "platform-flink", "platform-flink-ml", "platform-feast",
    "platform-mlflow", "platform-train", "platform-return-risk", "platform-feature-api",
    "platform-jenkins",
  ]) : toset([])

  name                 = "${var.name_prefix}/${each.key}"
  image_tag_mutability = "IMMUTABLE"
  force_delete         = false
  image_scanning_configuration {
    scan_on_push = true
  }
  encryption_configuration {
    encryption_type = "AES256"
  }
}
