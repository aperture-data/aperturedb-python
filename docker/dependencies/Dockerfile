FROM ubuntu:22.04

ENV OPENCV_VERSION=4.3.0

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends \
       python3-venv

ENV DEBIAN_FRONTEND noninteractive
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN apt-get -y install build-essential git cmake python3.10-venv\
    libx264-* libx265-* libavcodec-dev libavformat-dev\
    pkg-config\
    libavutil-dev libswscale-dev python3-venv\
    libavcodec-extra libavcodec-dev python3-dev\
    ffmpeg h264enc npm wget

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
        -D CMAKE_INSTALL_PREFIX=/usr/local .. &&        \
    make -j6 && make install
    # rm -rf /opencv-$OPENCV_VERSION

RUN pip install jupyterlab jupyterlab-dash dash-cytoscape plotly jupyter-dash numpy

RUN cd /opencv-$OPENCV_VERSION/build/python_loader && pip install -e .
