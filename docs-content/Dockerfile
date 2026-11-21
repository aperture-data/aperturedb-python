FROM alpine:3
RUN apk add --no-cache bash rsync
COPY aperturedb/ /content/aperturedb/
COPY examples/ /content/examples/
COPY docs/SDK1Migration.md /content/docs/SDK1Migration.md
# CI supplies the exact Git revision used for source/Colab links.
ARG SOURCE_REVISION=
RUN printf '%s\n' "$SOURCE_REVISION" > /content/.docs-source-revision
COPY docs-content/publish.sh /publish.sh
ENV DOCS_PROVIDER=aperturedb-python
ENTRYPOINT ["bash", "/publish.sh"]
