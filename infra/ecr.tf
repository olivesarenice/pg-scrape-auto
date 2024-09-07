resource "aws_ecr_repository" "repo" {
  name                 = "pg-scrape-auto"
  image_tag_mutability = "MUTABLE"
  encryption_configuration {
    encryption_type = "KMS"
  }
  tags = {
    Name = "pg-scrape-auto"
  }
}

resource "aws_ecr_lifecycle_policy" "repo_policy" {
  repository = aws_ecr_repository.repo.name

  policy = <<EOF
  {
    "rules": [
      {
        "rulePriority": 1,
        "description": "Expire untagged images older than 3 days",
        "selection": {
          "tagStatus": "untagged",
          "countType": "sinceImagePushed",
          "countUnit": "days",
          "countNumber": 3
        },
        "action": {
          "type": "expire"
        }
      }
    ]
  }
  EOF
}
