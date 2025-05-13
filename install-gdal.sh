#!/bin/bash

# Source helpers if available
source ./helpers.sh

# Function to check and install GDAL
install_gdal() {
    echo "Checking GDAL installation..."
    
    # Check if GDAL is installed
    if ! command_exists gdal-config; then
        echo -e "${RED}GDAL not found. Please install GDAL first.${NC}"
        echo "For macOS: brew install gdal"
        echo "For Ubuntu: sudo apt-get install libgdal-dev"
        return 1
    fi
    
    # Get GDAL version
    GDAL_VERSION=$(gdal-config --version)
    echo "Detected GDAL version: $GDAL_VERSION"
    
    # Install the corresponding Python GDAL package
    echo "Installing Python GDAL package version $GDAL_VERSION..."
    
    # Install the exact version
    if pip3 install gdal==$GDAL_VERSION; then
        echo -e "${GREEN}GDAL Python package installed successfully! ✓${NC}"
    else
        echo -e "${RED}Failed to install GDAL Python package.${NC}"
        echo "Please check if the GDAL Python package version $GDAL_VERSION is available."
        return 1
    fi
    
    return 0
}