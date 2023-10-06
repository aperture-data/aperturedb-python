set -e

source $(dirname "$0")/version.sh

check_for_changed_docker_files() {
  echo "Checking for changed docker files..."

  # Get files changed on merge
  FILES_CHANGED=$(git diff origin/${TARGET_BRANCH_NAME} origin/${BRANCH_NAME} --name-only | { grep 'Dockerfile' || true; })

  echo "Files Changed: " ${FILES_CHANGED}
  if [ -z "$FILES_CHANGED" ]
  then
    echo "No Dockerfile changes."
    return
  fi

  for file in $FILES_CHANGED; do

    # Check if dependencies image changed
    if [ $file == 'docker/dependencies/Dockerfile' ]
    then
      DEPENDENCIES_DOCKER_IMAGE_CHANGED=1
      echo "Dependencies image changed"
    fi
  done
  echo "Checking for changed docker files...done"
}

# Check and updates version based on release branch name
update_version() {
    echo "Checking versions"
    if [[ $BRANCH_NAME != "release"* ]]; then
        echo "Not release branch - skipping version update"
        return
    fi
    IFS=. read MAJOR_V MINOR_V MICRO_V <<<"${BRANCH_NAME##release-}"
    if [ -z "$MAJOR_V" ]; then
        echo "Missing major version"
        exit 1
    fi
    if [ -z "$MINOR_V" ]; then
        echo "Missing minor version"
        exit 1
    fi
    if [ -z "$MICRO_V" ]; then
        echo "Missing micro version"
        exit 1
    fi
    VERSION_BUMP=$MAJOR_V.$MINOR_V.$MICRO_V
    if [ $BUILD_VERSION == $VERSION_BUMP ]; then
        echo "Versions match - skipping update"
        return
    fi
    echo "Updating version $BUILD_VERSION to $VERSION_BUMP"
    # Replace version in __init__.py
    printf '%s\n' "%s/__version__ = .*/__version__ = \"$VERSION_BUMP\"/g" 'x' | ex aperturedb/__init__.py

    # Commit and push version bump
    git config --local user.name "github-actions[bot]"
    git config --local user.email "41898282+github-actions[bot]@users.noreply.github.com"
    git add ./aperturedb/__init__.py
    git commit -m "Version bump: ${BUILD_VERSION} to ${VERSION_BUMP}"
    git push --set-upstream origin $BRANCH_NAME
    BUILD_VERSION=$VERSION_BUMP
}

install_prerequisites() {
    sudo apt-get update
    sudo apt-get install -y vim awscli
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

#Install pre requisites
install_prerequisites

echo "Branch: $BRANCH_NAME"
if [ -z "$BRANCH_NAME" ]
then
    echo "This is on a merge branch. Will not continue"
    exit 0
fi

# Set default version to develop
BUILD_VERSION=develop

# Trigger read version
read_version
echo "Build version: $BUILD_VERSION"

if [ -z ${UPDATE_BRANCH+x} ]
then
    echo "UPDATE_BRANCH is not set"
else
    # Trigger update version
    update_version
fi

# Set image extension according to branch
if [ $BRANCH_NAME == 'main' ]
then
    IMAGE_EXTENSION_WITH_VERSION=":v${BUILD_VERSION}"
    IMAGE_EXTENSION_LATEST=":latest"
    DEPLOY_SERVER=yes
elif [ $BRANCH_NAME == 'develop' ]
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
echo "Repository: $DOCKER_REPOSITORY"

build_tests(){
    TESTS_IMAGE=$DOCKER_REPOSITORY/aperturedb-python-tests:latest
    mkdir -p docker/tests/aperturedata
    sudo rm -rf test/aperturedb/db
    cp -r aperturedb pyproject.toml README.md docker/tests/aperturedata
    mkdir -m 777 -p docker/tests/aperturedata/test/aperturedb
    cp -r test/*.py test/*.sh test/input docker/tests/aperturedata/test

    echo "Building image $TESTS_IMAGE"
    docker build -t $TESTS_IMAGE --cache-from $TESTS_IMAGE -f docker/tests/Dockerfile .
}

build_complete(){
    COMPLETE_IMAGE=$DOCKER_REPOSITORY/aperturedb-python-tests:complete
    mkdir -p docker/complete/aperturedata
    cp -r aperturedb pyproject.toml README.md LICENSE

    echo "Building image $COMPLETE_IMAGE"
    docker build -t $COMPLETE_IMAGE --cache-from $COMPLETE_IMAGE -f docker/complete/Dockerfile .
}

build_notebook_dependencies_image(){
    DEPS_IMAGE=$DOCKER_REPOSITORY/aperturedb-notebook:dependencies
    echo "Building image $DEPS_IMAGE"
    docker pull $DEPS_IMAGE
    docker build -t $DEPS_IMAGE --cache-from $DEPS_IMAGE -f docker/dependencies/Dockerfile .
    if [ -z ${NO_PUSH+x} ]
    then
        docker push --all-tags $DOCKER_REPOSITORY/aperturedb-notebook
    fi
}

# Build notebook image
build_notebook_image(){
    NOTEBOOK_IMAGE=$DOCKER_REPOSITORY/aperturedb-notebook${IMAGE_EXTENSION_WITH_VERSION}
    mkdir -p docker/notebook/aperturedata
    cp -r aperturedb pyproject.toml README.md docker/notebook/aperturedata
    LATEST_IMAGE=$DOCKER_REPOSITORY/aperturedb-notebook${IMAGE_EXTENSION_LATEST}
    echo "Building image $NOTEBOOK_IMAGE"
    docker build -t $NOTEBOOK_IMAGE -t $LATEST_IMAGE -f docker/notebook/Dockerfile .
    if [ -z ${NO_PUSH+x} ]
    then
        docker push --all-tags $DOCKER_REPOSITORY/aperturedb-notebook
    fi
}

build_coverage_image(){
    COV_IMAGE=$DOCKER_REPOSITORY/aperturedb-python-coverage${IMAGE_EXTENSION_WITH_VERSION}
    echo "Building image $COV_IMAGE"
    docker build -t $COV_IMAGE -f coverage/Dockerfile .
    if [ -z ${NO_PUSH+x} ]
    then
        docker push $COV_IMAGE

        ECR_REPO_NAME=aperturedb-python-coverage
        COV_IMAGE=$DOCKER_REPOSITORY/$ECR_REPO_NAME${IMAGE_EXTENSION_WITH_VERSION}
        ECR_NAME=$ECR_REPO_NAME:v$BUILD_VERSION
        push_aws_ecr $COV_IMAGE $ECR_NAME $ECR_REPO_NAME
    fi
}

push_aws_ecr(){
    SRC_IMAGE=$1
    DST_IMAGE=$2
    ECR_REPO_NAME=$3
    REGION=us-west-2
    PREFIX="aperturedata/"
    docker tag $SRC_IMAGE \
        684446431133.dkr.ecr.$REGION.amazonaws.com/$DST_IMAGE
    aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin 684446431133.dkr.ecr.$REGION.amazonaws.com

    aws ecr create-repository --repository-name $ECR_REPO_NAME --region us-west-2  || true

    docker push 684446431133.dkr.ecr.$REGION.amazonaws.com/$DST_IMAGE
}

deploy_terraform(){
    if [ "${DEPLOY_SERVER}" == "yes" ]
    then
        pushd "./deploy"
        source login.sh "${BRANCH_NAME}" "${IMAGE_EXTENSION_WITH_VERSION}"
        terraform init
        terraform apply -auto-approve
        popd
    fi
}

if [ -z ${EXCLUDE_TESTING+x} ]
then
    check_for_changed_docker_files
    echo "DEPENDENCIES_DOCKER_IMAGE_CHANGED=$DEPENDENCIES_DOCKER_IMAGE_CHANGED"
    # Dependecies
    # TODO : Conditionally build.
    # Check if there is base image change
    if [ "$DEPENDENCIES_DOCKER_IMAGE_CHANGED" == 1 ]
    then
        build_notebook_dependencies_image
        return
    fi

    build_notebook_image
    build_tests

    pushd "test"
    ./run_test_container.sh
    build_coverage_image
    rm -rf "./output/"
    popd
fi


if [ -z ${EXCLUDE_DEPLOY+x} ]
then
    deploy_terraform
fi

if [ -z ${EXCLUDE_BUILD_COMPLETE+x} ]
then
    build_complete
fi
