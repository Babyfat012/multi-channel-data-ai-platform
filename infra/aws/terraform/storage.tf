# Two separate buckets for raw and lakehouse; public access blocked, old versions kept for a limited time, tmp/ cleaned up.
data "aws_caller_identity" "current" {}

resource "aws_s3_bucket" "data" {
  for_each = toset(["raw", "lakehouse"])
  # S3 names are globally unique: append the account ID, like project_id on GCP.
  bucket        = "${var.name_prefix}-${data.aws_caller_identity.current.account_id}-${each.value}"
  force_destroy = var.force_destroy
}

resource "aws_s3_bucket_public_access_block" "data" {
  for_each                = aws_s3_bucket.data
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ACLs disabled; access goes only through IAM (like uniform_bucket_level_access).
resource "aws_s3_bucket_ownership_controls" "data" {
  for_each = aws_s3_bucket.data
  bucket   = each.value.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_versioning" "data" {
  for_each = aws_s3_bucket.data
  bucket   = each.value.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  for_each = aws_s3_bucket.data
  bucket   = each.value.id
  # Lifecycle must wait for versioning to be enabled before noncurrent rules can apply.
  depends_on = [aws_s3_bucket_versioning.data]

  rule {
    id     = "expire-noncurrent"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_retention_days
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  rule {
    id     = "expire-tmp"
    status = "Enabled"
    filter {
      prefix = "tmp/"
    }
    expiration {
      days = var.temp_retention_days
    }
  }
}
