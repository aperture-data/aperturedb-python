set -e
cd ..
source $(dirname "$0")/ci.sh
cd -


build_coverage_image(){
    echo "Preping coverage image"
    coverage html
    COV_IMAGE=$DOCKER_REPOSITORY/aperturedb-python-coverage${IMAGE_EXTENSION_WITH_VERSION}
    echo "Building image $COV_IMAGE"
    docker build -t $COV_IMAGE -f coverage/Dockerfile .
    if [ -z ${NO_PUSH+x} ]
    then
        docker push $COV_IMAGE
    fi
}

build_coverage_image

ECR_REPO_NAME=aperturedb-python-coverage
COV_IMAGE=$DOCKER_REPOSITORY/$ECR_REPO_NAME${IMAGE_EXTENSION_WITH_VERSION}
ECR_NAME=$ECR_REPO_NAME:v$BUILD_VERSION


push_aws_ecr $COV_IMAGE $ECR_NAME $ECR_REPO_NAME
