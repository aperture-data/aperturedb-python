FROM ubuntu:22.04
ARG VERSION
ENV DEBIAN_FRONTEND=noninteractive
COPY dist/aperturedb-${VERSION}-py3-none-any.whl /tmp
RUN apt update && apt upgrade -y && apt install -y --no-install-recommends \
    python3-dev \
    python3-pip
ARG VERSION
RUN pip3 install /tmp/aperturedb-${VERSION}-py3-none-any.whl
