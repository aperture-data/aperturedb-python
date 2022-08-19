set -e

source $(dirname "$0")/version.sh

read_version

echo "Configuring deployment with: $BUILD_VERSION"


find deploy/ -type f -name "*.yaml" -exec sed -i "s/\$VERSION/v$BUILD_VERSION/g" {} \;

