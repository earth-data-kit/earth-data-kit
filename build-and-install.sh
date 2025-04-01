#!/bin/bash

# Earth Data Kit (EDK) Installation Script

source ./check-prereq.sh

echo ""

echo "Building Earth Data Kit (EDK)..."

# Empty the dist directory before building to ensure we only have the latest version
echo "Cleaning dist directory..."
if [ -d "dist" ]; then
    rm -rf dist/*
    echo -e "${GREEN}Dist directory cleaned.${NC}"
else
    mkdir -p dist
    echo -e "${GREEN}Dist directory created.${NC}"
fi

echo ""

# Always build to ensure we have the latest version
if ! command_exists make; then
    echo -e "${RED}Make not found. Cannot build EDK.${NC}"
    echo "Please install make first."
    exit 1
fi

# Run make build to create the latest version
make build

echo ""
echo "Installing Earth Data Kit (EDK)..."
# Check if build was successful
LATEST_TAR=$(ls -t dist/earth_data_kit-*.tar.gz 2>/dev/null | head -1)
if [ -z "$LATEST_TAR" ]; then
    echo -e "${RED}Failed to build EDK.${NC}"
    exit 1
fi

# Extract version number from the tar filename
VERSION=$(echo "$LATEST_TAR" | sed -E 's/.*earth_data_kit-([0-9]+\.[0-9]+\.[0-9]+)\.tar\.gz/\1/')
echo -e "${GREEN}Found: $LATEST_TAR (version $VERSION). Installing...${NC}"

# Install EDK
pip3 install "$LATEST_TAR" -q

if [ $? -eq 0 ]; then
    echo -e "${GREEN}Earth Data Kit version $VERSION installed successfully!${NC}"
else
    echo -e "${RED}Installation failed.${NC}"
    exit 1
fi
