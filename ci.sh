set -e

# Read version from python code
read_version() {
   BUILD_VERSION=$(awk '$1=="__version__" && $2=="=" {print $3}' aperturedb/__init__.py | tr -d '"')
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
    DOCKER_REPOSITORY=ailegion
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
    mkdir -p docs/docker/build/docs
    cp -r ./{setup.py,README.md,aperturedb} docs/docker/build
    cp -r ./docs/{*.*,Makefile,_static} docs/docker/build/docs
    DOCKS_IMAGE=$DOCKER_REPOSITORY/aperturedb-python-docs${IMAGE_EXTENSION_WITH_VERSION}
    echo "Building image $DOCKS_IMAGE"
    docker build -t $DOCKS_IMAGE -f docs/docker/Dockerfile .
    if [ -z ${NO_PUSH+x} ]
    then
        docker push $DOCKS_IMAGE
    fi
}

# Trigger build notebook image
build_notebook_image

# Trigger build docs image
build_docs_image