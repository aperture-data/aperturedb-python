echo "Building aperturedb"
rm -rf build/ dist/ vdms.egg-info/
python3 -m build

echo "Uploading aperturedb"
twine upload --skip-existing --verbose dist/*

RELEASE_IMAGE="aperturedata/aperturedb-python:latest"
source version.sh && read_version
echo "Building image ${RELEASE_IMAGE}"
docker build --no-cache -t ${RELEASE_IMAGE} \
    --build-arg="VERSION=${BUILD_VERSION}" -f docker/release/Dockerfile .
docker push ${RELEASE_IMAGE}
