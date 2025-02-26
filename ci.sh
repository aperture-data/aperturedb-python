set -e

source $(dirname "${0}")/version.sh

# Check and updates version based on release branch name
update_version() {
    echo "Checking versions"
    if [[ "${BRANCH_NAME}" != "release"* ]]; then
        echo "Not release branch - skipping version update"
        return
    fi
    IFS=. read MAJOR_V MINOR_V MICRO_V <<<"${BRANCH_NAME##release-}"
    if [ -z "${MAJOR_V}" ]; then
        echo "Missing major version"
        exit 1
    fi
    if [ -z "${MINOR_V}" ]; then
        echo "Missing minor version"
        exit 1
    fi
    if [ -z "${MICRO_V}" ]; then
        echo "Missing micro version"
        exit 1
    fi
    VERSION_BUMP=${MAJOR_V}.${MINOR_V}.${MICRO_V}
    if [ "${BUILD_VERSION}" == "${VERSION_BUMP}" ]; then
        echo "Versions match - skipping update"
        return
    fi
    echo "Updating version ${BUILD_VERSION} to ${VERSION_BUMP}"
    # Replace version in __init__.py
    printf '%s\n' "%s/__version__ = .*/__version__ = \"${VERSION_BUMP}\"/g" 'x' | ex aperturedb/__init__.py

    # Commit and push version bump
    git config --local user.name "github-actions[bot]"
    git config --local user.email "41898282+github-actions[bot]@users.noreply.github.com"
    git add ./aperturedb/__init__.py
    git commit -m "Version bump: ${BUILD_VERSION} to ${VERSION_BUMP}"
    git push --set-upstream origin ${BRANCH_NAME}
    BUILD_VERSION=${VERSION_BUMP}
}

# Fetch branch
if [ -z ${BRANCH_NAME+x} ]
then
    BRANCH_NAME=$(git rev-parse --abbrev-ref HEAD)
fi

# Fetch branch
if [ -z ${COMMIT_HASH+x} ]
then
    COMMIT_HASH=$(git rev-parse HEAD)
fi

echo "Branch: ${BRANCH_NAME}"
if [ -z "${BRANCH_NAME}" ]
then
    echo "This is on a merge branch. Will not continue"
    exit 0
fi

# Set default version to develop
BUILD_VERSION=develop

# Trigger read version
read_version
echo "Build version: ${BUILD_VERSION}"

if [ "${UPDATE_BRANCH}" == "true" ]
then
    # Trigger update version
    update_version
else
    echo "UPDATE_BRANCH is not set"
fi

# Set image extension according to branch
if [ "${BRANCH_NAME}" == 'main' ]
then
    IMAGE_EXTENSION_WITH_VERSION=":v${BUILD_VERSION}"
    IMAGE_EXTENSION_LATEST=":latest"
    DEPLOY_SERVER=yes
elif [ "${BRANCH_NAME}" == 'develop' ]
then
    IMAGE_EXTENSION_WITH_VERSION="-develop:v${BUILD_VERSION}-${COMMIT_HASH}"
    DEPLOY_SERVER=yes
else
    IMAGE_EXTENSION_WITH_VERSION="-${BRANCH_NAME}:v${BUILD_VERSION}"
    DEPLOY_SERVER=no
fi

# Set default repo if repo var is not set
if [ -n ${DOCKER_REPOSITORY+x} ]
then
    DOCKER_REPOSITORY=aperturedata
fi
echo "Repository: ${DOCKER_REPOSITORY}"

build_tests(){
    TESTS_IMAGE=${DOCKER_REPOSITORY}/aperturedb-python-tests:latest
    mkdir -p docker/tests/aperturedata
    sudo rm -rf test/aperturedb/db
    cp -r aperturedb pyproject.toml README.md docker/tests/aperturedata
    mkdir -m 777 -p docker/tests/aperturedata/test/aperturedb
    cp -r test/*.py test/*.sh test/input docker/tests/aperturedata/test

    echo "Building image ${TESTS_IMAGE}"
    docker build -t ${TESTS_IMAGE} --cache-from ${TESTS_IMAGE} -f docker/tests/Dockerfile .
}

build_complete(){
    COMPLETE_IMAGE=${DOCKER_REPOSITORY}/aperturedb-python-tests:complete
    mkdir -p docker/complete/aperturedata
    cp -r aperturedb pyproject.toml README.md LICENSE docker/complete/aperturedata

    echo "Building image ${COMPLETE_IMAGE}"
    docker build -t ${COMPLETE_IMAGE} --cache-from ${COMPLETE_IMAGE} -f docker/complete/Dockerfile .
}

build_notebook_dependencies_image(){
    DEPS_IMAGE=${DOCKER_REPOSITORY}/aperturedb-notebook:dependencies

    if [ "${PULL_DEPENDENCIES}" != "false" ]
    then
        # Default
        # Build will use cache to speed up the process
        # Runs from github events
        cache_control="--cache-from ${DEPS_IMAGE}"
        docker pull ${DEPS_IMAGE}
    else
        # Build won't use cache to create a fresh image
        # Runs from cron-job
        cache_control="--no-cache"
    fi

    echo "Building image ${DEPS_IMAGE}"
    docker build -t ${DEPS_IMAGE} ${cache_control} -f docker/dependencies/Dockerfile .
    if [  "${PUSH_DEPENDENCIES}" != "true" ]
    then
        # Default
        # No need to push
        echo "Not pushing image ${DEPS_IMAGE}"
    else
        # Runs from cron-job
        echo "Pushing image ${DEPS_IMAGE}"
        docker push ${DEPS_IMAGE}
    fi
}

# Build notebook image
build_notebook_image(){
    NOTEBOOK_IMAGE=${DOCKER_REPOSITORY}/aperturedb-notebook${IMAGE_EXTENSION_WITH_VERSION}
    mkdir -p docker/notebook/aperturedata
    cp -r aperturedb pyproject.toml LICENSE README.md docker/notebook/aperturedata
    LATEST_IMAGE=${DOCKER_REPOSITORY}/aperturedb-notebook${IMAGE_EXTENSION_LATEST}
    CPU_IMAGE=${DOCKER_REPOSITORY}/aperturedb-notebook:cpu
    echo "Building image ${NOTEBOOK_IMAGE}"
    docker build -t ${NOTEBOOK_IMAGE} -t ${LATEST_IMAGE} -f docker/notebook/Dockerfile .
    docker build -t ${CPU_IMAGE} -f docker/notebook/Dockerfile.cpu .
    if [ "${NO_PUSH}" != "true" ]
    then
        docker push --all-tags ${DOCKER_REPOSITORY}/aperturedb-notebook
    fi
}

build_coverage_image(){
    COV_IMAGE=${DOCKER_REPOSITORY}/aperturedb-python-coverage${IMAGE_EXTENSION_WITH_VERSION}
    echo "Building image ${COV_IMAGE}"
    docker build -t ${COV_IMAGE} -f coverage/Dockerfile .
    if [ "${NO_PUSH}" != "true" ]
    then
        docker push ${COV_IMAGE}

        ECR_REPO_NAME=aperturedb-python-coverage
        COV_IMAGE=${DOCKER_REPOSITORY}/${ECR_REPO_NAME}${IMAGE_EXTENSION_WITH_VERSION}
        ECR_NAME=${ECR_REPO_NAME}:v${BUILD_VERSION}
        push_aws_ecr ${COV_IMAGE} ${ECR_NAME} ${ECR_REPO_NAME}
    fi
}

push_aws_ecr(){
    SRC_IMAGE=${1}
    DST_IMAGE=${2}
    ECR_REPO_NAME=${3}
    REGION=us-west-2
    PREFIX="aperturedata/"
    docker tag ${SRC_IMAGE} \
        684446431133.dkr.ecr.${REGION}.amazonaws.com/${DST_IMAGE}
    aws ecr get-login-password --region ${REGION} | docker login --username AWS --password-stdin 684446431133.dkr.ecr.${REGION}.amazonaws.com

    aws ecr create-repository --repository-name ${ECR_REPO_NAME} --region us-west-2  || true

    docker push 684446431133.dkr.ecr.${REGION}.amazonaws.com/${DST_IMAGE}
}

if [ "${RUN_TESTS}" == "true" ] || [ "${BUILD_DEPENDENCIES}" == "true" ]
then
    build_notebook_dependencies_image
fi

if [ "${RUN_TESTS}" == "true" ]
then
    build_notebook_image
    build_tests

    pushd "test"
    ./run_test_container.sh
    build_coverage_image
    rm -rf "./output/"
    popd
fi


if [ "${BUILD_COMPLETE}" == "true" ]
then
    build_complete
fi