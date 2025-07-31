# Set version as a variable
version="2.3.0"

get_s5cmd_filename() {
    # Get OS and ARCH from environment variables
    local os="${BUILD_PLATFORM:-}"
    local arch="${BUILD_ARCHS:-}"
    local filename=""

    case "$os" in
        macos)
            case "$arch" in
                arm64)
                    filename="s5cmd_${version}_macOS-arm64"
                    ;;
                x86_64)
                    filename="s5cmd_${version}_macOS-64bit"
                    ;;
                *)
                    echo "Unsupported architecture: $arch"
                    return 1
                    ;;
            esac
            ;;
        linux)
            case "$arch" in
                x86_64)
                    filename="s5cmd_${version}_Linux-64bit"
                    ;;
                aarch64)
                    filename="s5cmd_${version}_Linux-arm64"
                    ;;
                *)
                    echo "Unsupported architecture: $arch"
                    return 1
                    ;;
            esac
            ;;
        *)
            echo "Unsupported OS: $os"
            return 1
            ;;
    esac

    echo "$filename"
    return 0
}


filename=$(get_s5cmd_filename)
if [ -z "$filename" ]; then
    echo "Failed to get s5cmd filename."
    exit 1
fi

url="https://github.com/peak/s5cmd/releases/download/v${version}/${filename}.tar.gz"

echo "Downloading s5cmd from $url..."
wget -O /tmp/s5cmd.tar.gz "$url"
if [ $? -ne 0 ]; then
    echo "Failed to download s5cmd tarball."
    exit 1
fi

mkdir -p earth_data_kit/s5cmd/
tar -xzf /tmp/s5cmd.tar.gz -C earth_data_kit/s5cmd/
rm /tmp/s5cmd.tar.gz
