# Define the Lambda function
resource "aws_lambda_function" "transform" {
  function_name = "pg-scrape-auto-lambda"

  # Reference the Docker image stored in ECR
  image_uri = "${aws_ecr_repository.repo.repository_url}:cloud_lambda-0.1.0"

  role = aws_iam_role.lambda_role.arn

  # Set the Lambda function to use the Docker image
  package_type = "Image"

  # Define memory size (in MB)
  memory_size = 3008 # Set your desired memory size (e.g., 2048 MB)

  # Define function timeout (in seconds)
  timeout = 900 # Set your desired timeout (e.g., 600 seconds)

  # Define ephemeral storage (in MB)
  ephemeral_storage {
    size = 2048 # Set your desired ephemeral storage size (e.g., 2048 MB)
  }
}

# Output the Lambda function ARN
output "lambda_function_arn" {
  value = aws_lambda_function.transform.arn
}
