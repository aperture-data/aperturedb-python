FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt update && apt upgrade -y && apt install -y --no-install-recommends \
    python3-dev \
    python3-pip
ARG VERSION
COPY dist/aperturedb-${VERSION}-py3-none-any.whl /tmp
RUN pip3 install /tmp/aperturedb-${VERSION}-py3-none-any.whl
