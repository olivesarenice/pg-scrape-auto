#!/bin/zsh
set -e

# Print each command before execution for debugging
set -x

# Variables
APP_NAME=dashboard_app
PROJECT_NAME=pg-scrape-auto
ARCH=$1  # Pass the architecture as the first argument to the script

# Check if architecture argument is provided
if [ -z "$ARCH" ]; then
  echo "Error: Architecture argument is missing. Please specify 'arm64' or 'amd64'."
  exit 1
fi

if [ "$ARCH" != "arm64" ] && [ "$ARCH" != "amd64" ]; then
  echo "Error: Invalid architecture specified. Use 'arm64' or 'amd64'."
  exit 1
fi

# Get AWS Account ID and Region
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --output text | cut -f 1)
AWS_REGION=us-east-1

# Construct ECR URL and Image Tags
ECR_URL=${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com
VERSION_TAG=$(poetry version -s)
VERSIONED_IMAGE="$PROJECT_NAME:$APP_NAME-$VERSION_TAG-$ARCH"
IMAGE_URL="$ECR_URL/$VERSIONED_IMAGE"

# Print variables for debugging
echo "ECR URL: $ECR_URL"
echo "Versioned Image: $VERSIONED_IMAGE"
echo "Image URL: $IMAGE_URL"

# Build Docker Image with architecture-specific Dockerfile
docker buildx build --file Dockerfile_$ARCH -t $VERSIONED_IMAGE .

# Tag Docker Image
docker tag $VERSIONED_IMAGE $IMAGE_URL

# Login to AWS ECR
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $ECR_URL

# Push Docker Image
docker push $IMAGE_URL

# Confirm script completion
echo "Docker image pushed successfully."