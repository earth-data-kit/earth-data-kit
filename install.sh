#!/bin/bash

# Earth Data Kit (EDK) Installation Script

source ./check-prereq.sh

echo ""

# Install the latest version of EDK from the GitHub release using make
if ! command_exists make; then
    echo -e "${RED}Make not found. Cannot install EDK.${NC}"
    echo "Please install make first."
    exit 1
fi

make install-package