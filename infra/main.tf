provider "aws" {
  region = "us-east-1"
}

terraform {
  backend "s3" {
    bucket         = "oqsw-terraform-states"
    key            = "pg-scrape-auto/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "terraform-state-lock"
  }
}
