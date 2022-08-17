set -e

source $(dirname "$0")/version.sh

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
    printf '%s\n' "%s/__version__.*/__version__ = \"$VERSION_BUMP\"/g" 'x' | ex aperturedb/__init__.py
    printf '%s\n' "%s/version=.*/version=\"$VERSION_BUMP\",/g" 'x' | ex setup.py

    # Commit and push version bump
    git config --local user.name "github-actions[bot]"
    git config --local user.email "41898282+github-actions[bot]@users.noreply.github.com"
    git add ./aperturedb/__init__.py
    git add ./setup.py
    git commit -m "Version bump: ${BUILD_VERSION} to ${VERSION_BUMP}"
    git push --set-upstream origin $BRANCH_NAME
    BUILD_VERSION=$VERSION_BUMP
}

# Fetch branch
if [ -z ${BRANCH_NAME+x} ]
then
    BRANCH_NAME=$(git rev-parse --abbrev-ref HEAD)
fi

echo "Branch: $BRANCH_NAME"

# Set default version to develop
BUILD_VERSION=develop

# Trigger read version
read_version
echo "Build version: $BUILD_VERSION"

# Trigger update version
update_version

# Set image extension according to branch
IMAGE_EXTENSION_WITH_VERSION="-${BRANCH_NAME}:v${BUILD_VERSION}"
if [ $BRANCH_NAME == 'master' ] || [ $BRANCH_NAME == 'main' ]
then
    # when merging to master remove branch name
    IMAGE_EXTENSION_WITH_VERSION=":v${APP_VERSION}"
    IMAGE_EXTENSION_LATEST=":latest"
fi

# Set default repo if repo var is not set
if [ -n ${DOCKER_REPOSITORY+x} ]
then
    DOCKER_REPOSITORY=aperturedata
fi
echo "Repository: $DOCKER_REPOSITORY"

# Build notebook image
build_notebook_image(){
    NOTEBOOK_IMAGE=$DOCKER_REPOSITORY/aperturedb-notebook${IMAGE_EXTENSION_WITH_VERSION}
    echo "Building image $NOTEBOOK_IMAGE"
    docker build -t $NOTEBOOK_IMAGE -f docker/notebook/Dockerfile .
    if [ -z ${NO_PUSH+x} ]
    then
        docker push $NOTEBOOK_IMAGE
    fi
}

# Build docks image
build_docs_image(){
    echo "Preping docs image"
    mkdir -p docs/docker/build/{docs,examples}
    cp -r ./{setup.py,README.md,aperturedb} docs/docker/build
    cp -r ./docs/{*.*,Makefile,_static} docs/docker/build/docs
    find examples/ -name *.ipynb | xargs -i cp {} docs/docker/build/examples
    DOCS_IMAGE=$DOCKER_REPOSITORY/aperturedb-python-docs${IMAGE_EXTENSION_WITH_VERSION}
    echo "Building image $DOCS_IMAGE"
    docker build -t $DOCS_IMAGE -f docs/docker/Dockerfile .
    if [ -z ${NO_PUSH+x} ]
    then
        docker push $DOCS_IMAGE
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


# Execute only if ONLY_DEFINES is not set
if [ -z ${ONLY_DEFINES+x} ]
then
    # Trigger build notebook image
    build_notebook_image

    # Trigger build docs image
    build_docs_image

    ECR_REPO_NAME=aperturedb-python-docs
    DOCS_IMAGE=$DOCKER_REPOSITORY/$ECR_REPO_NAME${IMAGE_EXTENSION_WITH_VERSION}
    ECR_NAME=$ECR_REPO_NAME:v$BUILD_VERSION
    
    push_aws_ecr $DOCS_IMAGE $ECR_NAME $ECR_REPO_NAME 
fi


