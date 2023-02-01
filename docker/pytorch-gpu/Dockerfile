FROM gcr.io/deeplearning-platform-release/pytorch-gpu.1-13

RUN mkdir /aperturedata
ADD docker/tests/aperturedata /aperturedata

RUN cd /aperturedata && pip install -e . && pip install -r requirements.txt
RUN pip install opencv-python
RUN apt-get update && apt-get install -y libopencv-dev python3-opencv
COPY docker/tests/scripts/start.sh /start.sh
RUN  chmod 755 /start.sh
CMD ["/start.sh"]