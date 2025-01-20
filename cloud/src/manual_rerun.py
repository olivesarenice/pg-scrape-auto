import json
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta

import boto3
from dotenv import load_dotenv

# Load the environment variables from the .env file
load_dotenv()
region_name = "us-east-1"
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")


def find_empty_dates(bucket_name, target_prefix, start, end):
    # Create a session using the default AWS configuration (make sure your AWS credentials are set up)

    # Print to check if the credentials and region are set correctly
    print(region_name, aws_access_key_id, aws_secret_access_key)

    s3_client = boto3.client(
        "s3",
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    # Generate the date range (from N days ago to today)
    today = datetime.strptime(end, "%Y-%m-%d")
    start_date = datetime.strptime(start, "%Y-%m-%d")

    # Generate a list of dates from the start date to today
    date_range = []
    current_date = start_date
    while current_date <= today:
        date_range.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)
    # Create a set of all the dates in the range for easy checking
    date_set = set(date_range)

    # Create a dictionary to store prefix information (track file existence by date)
    date_dict = defaultdict(lambda: False)  # False means no files found for that date

    # List objects in the S3 bucket (with pagination support)
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=target_prefix)

    valid_dates = []
    for page in page_iterator:
        # Iterate over each object in the page
        for obj in page.get("Contents", []):

            # Check if the object key starts with the target prefix
            if obj["Key"].startswith(target_prefix) and obj["Key"].endswith(
                ".zip"  # "df.parquet"
            ):
                # Extract the date part from the file path, e.g., "02_zipped/2024/12/28/htmls.zip"
                parts = obj["Key"].split("/")
                if len(parts) >= 4:
                    year, month, day = parts[1], parts[2], parts[3]
                    date = f"{year}-{month}-{day}"
                    valid_dates.append(date)

    missing_dates = []
    for check_date in date_set:
        if check_date not in valid_dates:
            missing_dates.append(check_date)
    # # Find dates with no files
    # empty_dates = [date for date, has_files in date_dict.items() if not has_files]

    return sorted(missing_dates)


# Example usage:
bucket_name = "pg-scrape-auto"  # Replace with your actual bucket name
target_prefix = (
    "02_zipped"  # "03_transformed"  # The specific prefix/folder you're interested in
)
start = "2024-12-28"  # Set how many days back to check
end = "2025-01-20"
lambda_fn = "pg-scrape-auto-lambda"
backfill = False
# Get empty dates
empty_dates = find_empty_dates(bucket_name, target_prefix, start, end)

# Print the result (dates with no files)

if empty_dates:
    lambda_client = boto3.client(
        "lambda",
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    print("Dates with no files:")
    for date in empty_dates:
        print(date)
        if backfill:
            payload = {
                "step": "transform",
                "t": f"{date}",
            }
            response = lambda_client.invoke(
                FunctionName=lambda_fn,
                InvocationType="Event",  # Asynchronous invocation
                Payload=json.dumps(payload),
            )
            time.sleep(10)


else:
    print("All dates have files.")
