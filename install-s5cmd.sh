#!/bin/bash

# Source colors if available
source ./helpers.sh

# Function to install s5cmd on macOS
install_s5cmd_macos() {
    echo "Installing s5cmd on macOS..."
    
    if command_exists brew; then
        echo "Installing s5cmd using Homebrew..."
        brew install s5cmd
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}s5cmd installed successfully! ✓${NC}"
            return 0
        else
            echo -e "${RED}Failed to install s5cmd using Homebrew.${NC}"
            return 1
        fi
    else
        echo -e "${RED}Homebrew not found. Please install Homebrew first.${NC}"
        echo "Visit https://brew.sh for installation instructions."
        return 1
    fi
}

# Function to install s5cmd on Ubuntu/Linux
install_s5cmd_ubuntu() {
    echo "Installing s5cmd on Linux..."
    
    # Detect architecture
    ARCH=$(uname -m)
    
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
    
    # Create a temporary directory for download
    TMP_DIR=$(mktemp -d)
    cd $TMP_DIR
    
    # Use specific version URL instead of latest release
    S5CMD_URL="https://github.com/peak/s5cmd/releases/download/v2.3.0/s5cmd_2.3.0_Linux-64bit.tar.gz"
    
    echo "Downloading s5cmd from $S5CMD_URL..."
    curl -L -o s5cmd.tar.gz $S5CMD_URL
    
    # Extract to home directory
    S5CMD_DIR="$HOME/s5cmd"
    mkdir -p $S5CMD_DIR
    tar -xzf s5cmd.tar.gz -C $S5CMD_DIR
    
    # Add to PATH for current session
    export PATH="$S5CMD_DIR:$PATH"
    
    # Add to .bashrc for future sessions
    if ! grep -q "s5cmd" $HOME/.bashrc; then
        echo 'export PATH="$HOME/s5cmd:$PATH"' >> $HOME/.bashrc
        echo "Added s5cmd to PATH in .bashrc"
    fi
    
    # Clean up
    cd - > /dev/null
    rm -rf $TMP_DIR
    
    echo -e "${GREEN}s5cmd installed successfully! ✓${NC}"
    echo "Please restart your terminal or run 'source ~/.bashrc' to use s5cmd"
    return 0
}

# Function to install s5cmd based on architecture and OS
install_s5cmd() {
    echo "Checking system architecture and OS..."
    
    # Detect OS
    OS=$(uname -s)
    echo "Detected OS: $OS"
    
    # Install based on OS
    if [ "$OS" = "Darwin" ]; then
        # macOS installation
        install_s5cmd_macos
        return $?
    elif [ "$OS" = "Linux" ]; then
        # Linux installation
        install_s5cmd_ubuntu
        return $?
    else
        echo -e "${RED}Unsupported operating system: $OS${NC}"
        return 1
    fi
}