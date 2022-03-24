FROM ubuntu:20.04 as docs

COPY build build

RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
        build-essential autoconf automake libtool make g++ \
        graphviz python3-venv

RUN cd build/docs && bash gen_docs.sh

FROM nginx:alpine
COPY --from=docs /build/docs/_build/html /usr/share/nginx/html
