# Pull base image.
FROM aperturedata/aperturedb-notebook:dependencies

RUN mkdir /aperturedata
ADD docker/tests/aperturedata /aperturedata

RUN pip install awscli fuse libfuse-dev
RUN cd /aperturedata && pip install -e ".[dev]"
COPY docker/tests/scripts/start.sh /start.sh
RUN  chmod 755 /start.sh
CMD ["/start.sh"]