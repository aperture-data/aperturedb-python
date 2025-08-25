# Pull base image.
FROM aperturedata/aperturedb-notebook:dependencies

ARG PIP_INDEX_URL=https://pypi.org/simple
ARG PIP_TRUSTED_HOST=pypi.org

RUN mkdir /aperturedata
ADD docker/complete/aperturedata /aperturedata

RUN cd /aperturedata && PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST} PIP_INDEX_URL=${PIP_INDEX_URL} pip install -e ".[complete]"
