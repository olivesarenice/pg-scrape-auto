# Create IAM user
resource "aws_iam_user" "localmachine" {
  name = "pg-scrape-auto-localmachine"
}

# Create IAM policy to allow read/write access to the S3 bucket
resource "aws_iam_policy" "localmachine_s3_policy" {
  name        = "pg-scrape-auto-localmachine-s3-policy"
  description = "Policy to allow read and write access to pg-scrape-auto bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::pg-scrape-auto",
          "arn:aws:s3:::pg-scrape-auto/*",
          "arn:aws:s3:::oliverqsw-site-975050318877/external/public_datasets/pg-scrape-auto/",
          "arn:aws:s3:::oliverqsw-site-975050318877/external/public_datasets/pg-scrape-auto/*"
        ]
      }
    ]
  })
}

# Attach the policy to the user
resource "aws_iam_user_policy_attachment" "localmachine_policy_attachment" {
  user       = aws_iam_user.localmachine.name
  policy_arn = aws_iam_policy.localmachine_s3_policy.arn
}

# Optionally, create access keys for the user
resource "aws_iam_access_key" "localmachine_user_key" {
  user = aws_iam_user.localmachine.name
}


# Define IAM role for Lambda execution
resource "aws_iam_role" "lambda_role" {
  name = "pg-scrape-auto-lambda"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      },
    ]
  })
}

# Attach the basic Lambda execution policy to the role
resource "aws_iam_role_policy_attachment" "lambda_execution" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}
resource "aws_iam_role_policy_attachment" "lambda_s3" { # From the iam.tf definition
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.localmachine_s3_policy.arn
}
