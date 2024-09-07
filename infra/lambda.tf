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

# Define the Lambda function
resource "aws_lambda_function" "lambda" {
  function_name = "pg-scrape-auto-lambda"

  # Reference the Docker image stored in ECR
  image_uri = "${aws_ecr_repository.repo.repository_url}:cloud_lambda-0.1.0"

  role = aws_iam_role.lambda_role.arn

  # Set the Lambda function to use the Docker image
  package_type = "Image"

  # Optionally, define memory size and timeout
  memory_size = 1024
  timeout     = 600
}

# Output the Lambda function ARN
output "lambda_function_arn" {
  value = aws_lambda_function.lambda.arn
}
