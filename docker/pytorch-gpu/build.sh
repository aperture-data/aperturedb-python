if [ "${NO_CACHE:-false}" = "true" ]; then
    CACHE_FLAG="--no-cache"
else
    CACHE_FLAG=""
fi

docker build --pull $CACHE_FLAG -f docker/pytorch-gpu/Dockerfile -t aperturedata/aperturedb-pytorch-gpu:latest .