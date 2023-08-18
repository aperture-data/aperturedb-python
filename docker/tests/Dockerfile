# Pull base image.
FROM aperturedata/aperturedb-notebook:dependencies

RUN mkdir /aperturedata
ADD docker/tests/aperturedata /aperturedata

RUN pip install awscli
RUN cd /aperturedata && pip install -e . && pip install -r requirements.txt && ( cd minimal && pip install -e . )
COPY docker/tests/scripts/start.sh /start.sh
RUN  chmod 755 /start.sh
CMD ["/start.sh"]
