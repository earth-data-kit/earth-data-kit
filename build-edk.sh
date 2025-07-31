# Get platform and arch from environment variables
platform="${BUILD_PLATFORM:-}"
arch="${BUILD_ARCHS:-}"

if [ -z "$platform" ] || [ -z "$arch" ]; then
    echo "Error: BUILD_PLATFORM and BUILD_ARCHS environment variables must be set."
    exit 1
fi

bash get-s5cmd.sh

rm -rf build
rm -rf earth_data_kit.egg-info


if [ "$platform" == "linux" ]; then
    CIBW_BEFORE_ALL="bash before-build.sh gdal-devel" CIBW_REPAIR_WHEEL_COMMAND="" CIBW_ARCHS=$arch CIBW_BUILD="cp312-* cp313-*" cibuildwheel --platform $platform --output-dir dist
elif [ "$platform" == "macos" ]; then
    CIBW_ARCHS=$arch CIBW_BUILD="cp312-* cp313-*" CIBW_REPAIR_WHEEL_COMMAND="" cibuildwheel --platform $platform --output-dir dist
else
    echo "Error: Unsupported OS/platform: $platform"
    exit 1
fi