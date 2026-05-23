FROM gcr.io/deeplearning-platform-release/pytorch-gpu.py310:m130

RUN mkdir /aperturedata
ADD docker/pytorch-gpu/aperturedata /aperturedata

RUN pip install awscli
RUN apt-get update && apt-get install -y libopencv-dev python3-opencv fuse libfuse-dev
RUN cd /aperturedata && pip install -e ".[dev]"
RUN pip install git+https://github.com/openai/CLIP.git@d05afc436d78f1c48dc0dbf8e5980a9d471f35f6

COPY docker/pytorch-gpu/scripts/start.sh /start.sh
RUN  chmod 755 /start.sh
CMD ["/start.sh"]