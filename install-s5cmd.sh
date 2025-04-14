#!/bin/bash

# Source colors if available
source ./helpers.sh

# Function to install s5cmd based on architecture and OS
install_s5cmd() {
    echo "Checking system architecture and OS..."
    
    # Detect OS
    OS=$(uname -s)
    # Detect architecture
    ARCH=$(uname -m)
    
    echo "Detected OS: $OS"
    echo "Detected architecture: $ARCH"
    
    # Map architecture to s5cmd naming convention
    case $ARCH in
        x86_64)
            S5CMD_ARCH="amd64"
            ;;
        aarch64|arm64)
            S5CMD_ARCH="arm64"
            ;;
        *)
            echo -e "${RED}Unsupported architecture: $ARCH${NC}"
            return 1
            ;;
    esac
    
    # Install based on OS
    if [ "$OS" = "Darwin" ]; then
        # macOS installation
        if command_exists brew; then
            echo "Installing s5cmd using Homebrew..."
            brew install s5cmd
            if [ $? -eq 0 ]; then
                echo -e "${GREEN}s5cmd installed successfully! ✓${NC}"
                return 0
            else
                echo -e "${RED}Failed to install s5cmd using Homebrew.${NC}"
                exit 1
            fi
        fi
        
    elif [ "$OS" = "Linux" ]; then
        # Linux installation
        echo -e "${RED}Linux installation is not supported.${NC}"
        echo "Please follow installation instructions at https://github.com/peak/s5cmd"
        return 1
    else
        echo -e "${RED}Unsupported operating system: $OS${NC}"
        return 1
    fi
}