FROM gcr.io/deeplearning-platform-release/pytorch-gpu.1-13

RUN mkdir /aperturedata
ADD docker/pytorch-gpu/aperturedata /aperturedata

RUN pip install awscli
RUN cd /aperturedata && pip install -e . && pip install -r requirements.txt && ( cd minimal && pip install -e . )
RUN pip install opencv-python
RUN apt-get update && apt-get install -y libopencv-dev python3-opencv
COPY docker/pytorch-gpu/scripts/start.sh /start.sh
RUN  chmod 755 /start.sh
CMD ["/start.sh"]
