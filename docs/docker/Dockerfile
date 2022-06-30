FROM ubuntu:20.04 as docs

RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
        build-essential autoconf automake libtool make g++ \
        graphviz pandoc python3-venv

COPY docs/docker/build build

RUN cd build/docs && bash gen_docs.sh

FROM nginx:alpine
COPY --from=docs /build/docs/_build/html /usr/share/nginx/html
