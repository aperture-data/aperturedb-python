FROM alpine:3
RUN apk add --no-cache bash rsync
COPY aperturedb/ /content/aperturedb/
COPY examples/ /content/examples/
COPY docs-content/publish.sh /publish.sh
ENV DOCS_PROVIDER=aperturedb-python
ENTRYPOINT ["bash", "/publish.sh"]
