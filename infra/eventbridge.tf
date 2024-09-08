# Define the EventBridge rule
resource "aws_cloudwatch_event_rule" "daily_transform" {
  name                = "pg-scrape-auto-daily-transform"
  description         = "Triggers the Lambda function daily at 15:00 UTC"
  schedule_expression = "cron(0 15 * * ? *)" # Cron expression for 15:00 UTC daily
}

# Define the Lambda function target for the EventBridge rule
resource "aws_cloudwatch_event_target" "transform" {
  rule = aws_cloudwatch_event_rule.daily_transform.name
  arn  = aws_lambda_function.transform.arn # Reference the Lambda function

  # Define the input JSON payload
  input = jsonencode({
    step = "transform"
  })
}

# Allow EventBridge to invoke the Lambda function
resource "aws_lambda_permission" "eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.transform.function_name
  principal     = "events.amazonaws.com"

  # The source ARN is the ARN of the EventBridge rule
  source_arn = aws_cloudwatch_event_rule.daily_transform.arn
}
