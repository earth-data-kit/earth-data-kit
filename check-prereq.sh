#!/bin/bash

source ./helpers.sh
# Script to check the pre-requisites for the earth-data-kit

# Check GDAL installation
echo "Checking GDAL installation..."
if ! command_exists gdal-config; then
    echo -e "${RED}GDAL not found. Please install GDAL 3.10 or above.${NC}"
    echo "Installation instructions: https://gdal.org/download.html"
    exit 1
fi

# Check GDAL version
GDAL_VERSION=$(gdal-config --version)
GDAL_MAJOR=$(echo $GDAL_VERSION | cut -d. -f1)
GDAL_MINOR=$(echo $GDAL_VERSION | cut -d. -f2)

if [ "$GDAL_MAJOR" -lt 3 ] || ([ "$GDAL_MAJOR" -eq 3 ] && [ "$GDAL_MINOR" -lt 10 ]); then
    echo -e "${RED}GDAL version $GDAL_VERSION is too old. Version 3.10 or above is required.${NC}"
    echo "Please upgrade your GDAL installation."
    exit 1
else
    echo -e "${GREEN}GDAL version $GDAL_VERSION found. ✓${NC}"
fi

echo ""
# Check s5cmd installation
echo "Checking s5cmd installation..."
if ! command_exists s5cmd; then
    echo -e "${YELLOW}s5cmd not found. This is required for optimal S3 operations.${NC}"
    echo "Installation instructions: https://github.com/peak/s5cmd#installation"
    
    read -p "Do you want to install s5cmd now? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        # Source the install script and call the function
        source ./install-s5cmd.sh
        install_s5cmd
    else
        read -p "Do you want to continue without s5cmd? (y/n) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    fi
else
    echo -e "${GREEN}s5cmd found. ✓${NC}"
fi

echo ""
# Check operating system
echo "Checking operating system..."
OS=$(uname -s)
if [ "$OS" != "Darwin" ]; then
    echo -e "${RED}Error: Operating system detected as $OS. Earth Data Kit only supports macOS (Darwin) currently.${NC}"
    echo "Installation cannot continue on this operating system."
    exit 1
else
    echo -e "${GREEN}Operating system: macOS (Darwin) ✓${NC}"
fi

echo ""
echo -e "${GREEN}All pre-requisites installed. ✓${NC}"