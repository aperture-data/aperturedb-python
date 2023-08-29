#! /bin/bash

source $(dirname "$0")/version.sh

# Set default version to develop
BUILD_VERSION=develop

# Trigger read version
read_version
echo "Build version: $BUILD_VERSION"

create_release() {
    user="aperture-data"
    repo="aperturedb-python"
    token=$TOKEN
    tag="v$BUILD_VERSION"

    command="curl -s -o release.json -w '%{http_code}' \
         --request POST \
         --header 'Accept: application/vnd.github+json' \
         --header 'Authorization: Bearer ${token}' \
         --header 'X-GitHub-Api-Version: 2022-11-28' \
         --data '{\"tag_name\": \"${tag}\", \"name\": \"${tag}\", \"body\":\"Release ${tag}\"}' \
         https://api.github.com/repos/$user/$repo/releases"
    http_code=`eval $command`
    if [ $http_code == "201" ]; then
        echo "created release:"
        cat release.json
    else
        echo "create release failed with code '$http_code':"
        cat release.json
        echo "command:"
        echo $command
        return 1
    fi
}

create_release