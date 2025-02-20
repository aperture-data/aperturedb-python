set -e

echo "Building aperturedb"
rm -rf build/ dist/ vdms.egg-info/

docker build --no-cache -t CI/twine -f docker/twine/Dockerfile .
echo "Uploading aperturedb"

docker rm -f publisher || true
docker run --rm --name publisher \
  -e "TWINE_USERNAME=${TWINE_USERNAME}" \
  -e "TWINE_PASSWORD=${TWINE_PASSWORD}" \
  -v ./:/publish \
  CI/twine bash -c "cd /publish && python -m build && twine upload --skip-existing --verbose dist/*"

RELEASE_IMAGE="aperturedata/aperturedb-python:latest"
source version.sh && read_version
echo "Building image ${RELEASE_IMAGE}"
docker build --no-cache -t ${RELEASE_IMAGE} \
    --build-arg="VERSION=${BUILD_VERSION}" -f docker/release/Dockerfile .
docker push ${RELEASE_IMAGE}
