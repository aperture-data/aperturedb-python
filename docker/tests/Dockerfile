# Pull base image.
FROM aperturedata/aperturedb-notebook:dependencies

ARG PIP_INDEX_URL=https://pypi.org/simple
ARG PIP_TRUSTED_HOST=pypi.org

RUN mkdir /aperturedata
ADD docker/tests/aperturedata /aperturedata

RUN pip install awscli
RUN cd /aperturedata && PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST} PIP_INDEX_URL=${PIP_INDEX_URL} pip install -e ".[dev]"
RUN PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST} PIP_INDEX_URL=${PIP_INDEX_URL} pip install git+https://github.com/openai/CLIP.git
COPY docker/tests/scripts/start.sh /start.sh
RUN  chmod 755 /start.sh
CMD ["/start.sh"]