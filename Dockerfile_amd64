

# Use the official AWS Lambda Python base image
FROM public.ecr.aws/lambda/python:3.12

# Set the working directory in the container
WORKDIR /var/task

# Install Poetry
RUN pip install poetry

# Copy the poetry configuration files and install dependencies
COPY pyproject.toml poetry.lock* ./
RUN poetry config virtualenvs.create false && poetry install --no-dev

# Copy the Lambda function code into the container
COPY cloud/src ./
RUN rm -rf /var/task/tmp/

COPY cloud/src/tmp  /tmp
# Command to run the Lambda function
CMD ["main.lambda_handler"]