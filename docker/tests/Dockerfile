# Pull base image.
FROM aperturedata/aperturedb-notebook:dependencies

RUN mkdir /aperturedata
ADD docker/tests/aperturedata /aperturedata

RUN cd /aperturedata && pip install -e . && pip install -r requirements.txt
COPY docker/tests/scripts/start.sh /start.sh
RUN  chmod 755 /start.sh
CMD ["/start.sh"]