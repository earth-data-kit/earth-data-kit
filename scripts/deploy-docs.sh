#!/bin/bash
# Script to build and deploy Earth Data Kit documentation to S3

set -e  # Exit immediately if any command fails

# --- Configuration ---
BUILD_DIR="docs/build/html"
S3_BUCKET="s3://test-edk/earth-data-kit/"
REGION="ap-south-1"

echo "Starting Earth Data Kit docs deployment..."

# --- Step 1: Build documentation using Sphinx ---
echo "Building documentation..."
make build-docs

# --- Step 2: Verify build directory exists ---
if [ ! -d "$BUILD_DIR" ]; then
  echo "Build directory not found: $BUILD_DIR"
  exit 1
fi

# --- Step 3: Sync HTML files to S3 bucket ---
echo "Uploading docs to S3 bucket: $S3_BUCKET"
aws s3 sync "$BUILD_DIR" "$S3_BUCKET" --delete

# --- Step 4: Enable static website hosting on S3 ---
echo "Configuring S3 static website hosting..."
aws s3 website s3://test-edk/ --index-document index.html --error-document index.html

# --- Step 5: Confirm bucket policy status ---
echo "Checking bucket policy status..."
aws s3api get-bucket-policy-status --bucket test-edk || echo "No bucket policy found or not public yet."

# --- Step 6: Output the public endpoint ---
echo "Deployment complete!"
echo "Access your docs here: http://test-edk.s3-website.${REGION}.amazonaws.com/earth-data-kit/"
