#!/bin/zsh
set -e

# Print each command before execution for debugging
set -x

# Variables
JOB_NAME=cloud_lambda
PROJECT_NAME=pg-scrape-auto

# Get AWS Account ID and Region
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --output text | cut -f 1)
AWS_REGION=us-east-1

# Construct ECR URL and Image Tags
ECR_URL=${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com
VERSION_TAG=$(poetry version -s)
VERSIONED_IMAGE="$PROJECT_NAME:$JOB_NAME-$VERSION_TAG"
IMAGE_URL="$ECR_URL/$VERSIONED_IMAGE"

# Print variables for debugging
echo "ECR URL: $ECR_URL"
echo "Versioned Image: $VERSIONED_IMAGE"
echo "Image URL: $IMAGE_URL"

# Build Docker Image
docker buildx build . -t $VERSIONED_IMAGE

# Tag Docker Image
docker tag $VERSIONED_IMAGE $IMAGE_URL

# Login to AWS ECR
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $ECR_URL

# Push Docker Image
docker push $IMAGE_URL

# Confirm script completion
echo "Docker image pushed successfully."
