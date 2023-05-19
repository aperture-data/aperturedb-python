if [ -z ${DOCKER_CONFIG_FILE+x} ]; then
    DOCKER_CONFIG_FILE=~/.docker/config.json
    echo "DOCKER_CONFIG_FILE is not set, will use \"${DOCKER_CONFIG_FILE}\"."
fi
IFS=: read -r docker_username docker_password <<< \
    $(jq .auths.\"https://index.docker.io/v1/\".auth "${DOCKER_CONFIG_FILE}" | \
    tr -d '"' | base64 -d)
export TF_VAR_docker_username="${docker_username}"
if [ 0 -eq ${#TF_VAR_docker_username} ]; then
    echo "Error: Expected to get docker username from file $DOCKER_CONFIG_FILE"
fi
export TF_VAR_docker_password="${docker_password}"
if [ 0 -eq ${#TF_VAR_docker_password} ]; then
    echo "Error: Expected to get docker password from file $DOCKER_CONFIG_FILE"
fi
