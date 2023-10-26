echo "Building aperturedb"
rm -rf build/ dist/ vdms.egg-info/
python3 -m build

echo "Uploading aperturedb"
twine upload --skip-existing --verbose dist/*

RELEASE_IMAGE="aperturedata/aperturedb-python:latest"
echo "Building image ${RELEASE_IMAGE}"
docker build --no-cache -t ${RELEASE_IMAGE} -f docker/release/Dockerfile .
docker push ${RELEASE_IMAGE}
