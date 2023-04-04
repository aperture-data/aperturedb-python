#!/bin/bash

# Arguments:
    # 1. Branch
    # 2. Commit Hash

if [ $(pwd) != $(realpath $(dirname ${BASH_SOURCE[0]}})) ]; then
    echo "You have to source this script from its own folder."
    return
fi

source login-aws.sh
source login-docker.sh


if [ "${1}" != "develop" ] && [ "${1}" != "main" ]
then
    echo "Error: Environment is required. Pass \"develop\" or \"main\"."
    exit 1
fi

rm -rf .terraform
cat <<EOF > backend.tf
# This file is auto generated. Don't modify.

terraform {
  backend "s3" {
    bucket     = "aperturedata.infra.develop"
    key        = "python-web/terraform-${1}"
    access_key = "${TF_VAR_aws_access_key}"
    secret_key = "${TF_VAR_aws_secret_key}"
    region     = "us-west-2"
  }
}

locals {
  environment   = "${1}"
  image-ext-tag = "${2}"
}
EOF
