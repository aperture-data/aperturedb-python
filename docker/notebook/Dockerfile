# Pull base image.
FROM ubuntu:22.04

RUN    apt-get update \
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
    ffmpeg h264enc npm

RUN git clone https://github.com/opencv/opencv.git

RUN pip install numpy &&\
    cd /opencv && git clean -dfx && mkdir /build_cv && cd /build_cv &&\
    cmake /opencv . -D WITH_FFMPEG=ON && make -j 8

COPY docker/notebook/scripts/requirements.txt /requirements.txt
RUN pip install -r requirements.txt

RUN mkdir /aperturedata
COPY docker/notebook/aperturedata /aperturedata

COPY docker/notebook/scripts/set_notebook.sh /set_notebook.sh
RUN bash set_notebook.sh

# Configure the Juypter Notebook password to "test"
RUN jupyter lab --generate-config \
    && echo "c.NotebookApp.password='sha1:669c99384897:b5742e955addfc2ad3ce8c8faad4a92d55b0b55c' ">> /root/.jupyter/jupyter_notebook_config.py


COPY docker/notebook/scripts/start.sh /start.sh
RUN  chmod 755 /start.sh

# Add Tini. Tini operates as a process subreaper for jupyter.
# This prevents kernel crashes.
# ENV TINI_VERSION v0.6.0
# ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/bin/tini
# RUN chmod +x /usr/bin/tini
# ENTRYPOINT ["/usr/bin/tini", "--"]

EXPOSE 8888
CMD ["/start.sh"]
