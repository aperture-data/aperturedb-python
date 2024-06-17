FROM ubuntu:22.04

# Updated as per the newest release.
ENV OPENCV_VERSION=4.7.0

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends \
       python3-venv

ENV DEBIAN_FRONTEND noninteractive
ENV VIRTUAL_ENV=/opt/venv
ENV NODEJS_LTS=v20.12.2
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN apt-get -y install build-essential git cmake python3.10-venv\
    libx264-* libx265-* libavcodec-dev libavformat-dev\
    pkg-config\
    libavutil-dev libswscale-dev python3-venv\
    libavcodec-extra libavcodec-dev python3-dev\
    ffmpeg h264enc wget fuse libfuse-dev

#The version of nodejs in ubuntu is very old
#Installing the LTS as on Feb 23
RUN cd /opt && wget https://nodejs.org/dist/${NODEJS_LTS}/node-${NODEJS_LTS}-linux-x64.tar.xz && tar xf node-${NODEJS_LTS}-linux-x64.tar.xz
ENV PATH="/opt/node-${NODEJS_LTS}-linux-x64/bin:$PATH"

# Cmake determines the correct path for site packages by looking at
# numpy, and results in following output in configure:
# --   Python 3:
# --     Interpreter:                 /opt/venv/bin/python3 (ver 3.10.6)
# --     Libraries:                   /usr/lib/x86_64-linux-gnu/libpython3.10.so (ver 3.10.6)
# --     numpy:                       /opt/venv/lib/python3.10/site-packages/numpy/core/include (ver 1.24.1)
# --     install path:                lib/python3.10/site-packages/cv2/python-3.10
RUN pip install "numpy<2"

RUN wget -q https://github.com/opencv/opencv/archive/$OPENCV_VERSION.tar.gz && \
    tar xf $OPENCV_VERSION.tar.gz && rm $OPENCV_VERSION.tar.gz && \
    cd opencv-$OPENCV_VERSION && mkdir build && cd build && \
    cmake                                               \
        -D CMAKE_BUILD_TYPE=Release                     \
        -D WITH_TBB=OFF -D WITH_OPENMP=ON -D WITH_IPP=ON \
        -D CPU_DISPATCH=SSE4_2,AVX,AVX2                 \
        -D BUILD_EXAMPLES=OFF                           \
        -D BUILD_DOCS=OFF                               \
        -D BUILD_PERF_TESTS=OFF                         \
        -D BUILD_TESTS=OFF                              \
        -D BUILD_opencv_apps=OFF                        \
        -D WITH_FFMPEG=ON                               \
        -D CMAKE_INSTALL_PREFIX=/usr/local              \
        -D OPENCV_PYTHON3_INSTALL_PATH=/opt/venv/lib/python3.10/site-packages .. && \
    make -j6 && make install

RUN pip install jupyterlab jupyterlab-dash dash-cytoscape plotly jupyter-dash numpy
RUN jupyter lab build
RUN jupyter labextension disable "@jupyterlab/apputils-extension:announcements"
