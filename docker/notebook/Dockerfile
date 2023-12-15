# Pull base image.
FROM aperturedata/aperturedb-notebook:dependencies

RUN mkdir /aperturedata
ADD docker/notebook/aperturedata /aperturedata

COPY docker/notebook/scripts/start.sh /start.sh
RUN  chmod 755 /start.sh

# Add Tini. Tini operates as a process subreaper for jupyter.
# This prevents kernel crashes.
# ENV TINI_VERSION v0.6.0
# ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/bin/tini
# RUN chmod +x /usr/bin/tini
# ENTRYPOINT ["/usr/bin/tini", "--"]
RUN cd /aperturedata && pip install -e ".[notebook]"

EXPOSE 8888
CMD ["/start.sh"]
