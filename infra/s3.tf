resource "aws_s3_bucket" "data" {
  bucket = "pg-scrape-auto"

  tags = {
    project = "pg-scrape-auto"
    created = "terraform"
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket = aws_s3_bucket.data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
