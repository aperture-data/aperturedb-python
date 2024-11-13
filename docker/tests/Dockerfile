# Pull base image.
FROM aperturedata/aperturedb-notebook:dependencies

RUN mkdir /aperturedata
ADD docker/tests/aperturedata /aperturedata

RUN pip install awscli
RUN cd /aperturedata && pip install -e ".[dev]"
RUN pip install git+https://github.com/openai/CLIP.git
COPY docker/tests/scripts/start.sh /start.sh
RUN  chmod 755 /start.sh
CMD ["/start.sh"]