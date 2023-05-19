if [ -z ${AWS_SHARED_CREDENTIALS_FILE+x} ]; then
    echo "AWS_SHARED_CREDENTIALS_FILE is not set, will use the default credentials file."
fi
if [ -z ${AWS_DEFAULT_PROFILE+x} ]; then
    echo "AWS_DEFAULT_PROFILE is not set, will use the default profile."
fi
export TF_VAR_aws_access_key=$(aws configure get aws_access_key_id)
export TF_VAR_aws_secret_key=$(aws configure get aws_secret_access_key)
if [ 0 -eq ${#TF_VAR_aws_access_key} ] || [ 0 -eq ${#TF_VAR_aws_secret_key} ]; then
    if [ -z ${AWS_ACCESS_KEY_ID} ] || [ -z ${AWS_SECRET_ACCESS_KEY} ]; then
        echo "Error: Expected to get aws credentials from \"aws configure\" command."
    else
        export TF_VAR_aws_access_key=${AWS_ACCESS_KEY_ID}
        export TF_VAR_aws_secret_key=${AWS_SECRET_ACCESS_KEY}
    fi
fi
