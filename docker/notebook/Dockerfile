# Pull base image.
FROM aperturedata/aperturedb-notebook:dependencies

RUN mkdir /aperturedata
ADD docker/notebook/aperturedata /aperturedata

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
RUN cd /aperturedata && pip install -e .

EXPOSE 8888
CMD ["/start.sh"]
