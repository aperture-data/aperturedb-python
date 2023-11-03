FROM gcr.io/deeplearning-platform-release/pytorch-gpu.1-13.py37

RUN mkdir /aperturedata
ADD docker/pytorch-gpu/aperturedata /aperturedata

RUN pip install awscli
RUN apt-get update && apt-get install -y libopencv-dev python3-opencv fuse libfuse-dev
RUN cd /aperturedata && pip install -e ".[dev]"

COPY docker/pytorch-gpu/scripts/start.sh /start.sh
RUN  chmod 755 /start.sh
CMD ["/start.sh"]