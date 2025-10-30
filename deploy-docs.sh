#!/bin/bash
# Script to build and deploy Earth Data Kit documentation to S3

set -e  # Exit immediately if any command fails

# --- Configuration ---
BUILD_DIR="docs/build/html"
S3_BUCKET="s3://earthlabs.io/earth-data-kit/"
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

# --- Step 6: Output the public endpoint ---
echo "Deployment complete!"
echo "Access your docs here: https://earthlabs.io/earth-data-kit/"
