# Pull base image.
FROM aperturedata/aperturedb-notebook:dependencies

RUN mkdir /aperturedata
ADD docker/complete/aperturedata /aperturedata

RUN cd /aperturedata && pip install -e ".[complete]"
