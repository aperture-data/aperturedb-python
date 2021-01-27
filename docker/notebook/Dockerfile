# Pull base image.
FROM ubuntu:18.04

RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
        build-essential scons autoconf automake libtool make g++ \
        cmake git pkg-config \
        wget ed curl bzip2 libbz2-dev unzip bsdtar \
        openssh-client libssl-dev \
        libjsoncpp1 \
        libgflags2.2 libgoogle-glog0v5\
        libavcodec57 libavformat57 libswscale4 \
        libtbb2 \
        libgl1-mesa-glx \
        libssl1.1 \
        libjpeg8 libpng16-16 libtiff5 \
        libopenblas-base \
        libopenblas-dev && \
    apt-get remove -y libprotobuf-dev protobuf-compiler && \
    rm -rf /var/lib/apt/lists/* /root/.cache

RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y --no-install-recommends python3-pip && \
    pip3 install --upgrade pip

RUN pip3 install numpy && \
    pip3 install setuptools && \
    pip3 install notebook && \
    pip3 install matplotlib && \
    pip3 install opencv-python && \
    pip3 install image && \
    pip3 install scikit-image && \
    pip3 install aperturedb

# Configure the Juypter Notebook password to "test"
RUN jupyter notebook --generate-config && \
    echo "c.NotebookApp.password='sha1:669c99384897:b5742e955addfc2ad3ce8c8faad4a92d55b0b55c' ">> /root/.jupyter/jupyter_notebook_config.py

COPY scripts/start.sh /start.sh
RUN  chmod 755 /start.sh

# Add Tini. Tini operates as a process subreaper for jupyter.
# This prevents kernel crashes.
ENV TINI_VERSION v0.6.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/bin/tini
RUN chmod +x /usr/bin/tini
ENTRYPOINT ["/usr/bin/tini", "--"]

EXPOSE 8888
EXPOSE 55555
CMD ["/start.sh"]
