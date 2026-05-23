# Pull base image.
FROM aperturedata/aperturedb-notebook:dependencies
ARG PIP_INDEX_URL=https://pypi.org/simple
ARG PIP_TRUSTED_HOST=pypi.org

RUN mkdir /aperturedata

# Install CLIP (for running transformers).
# Use the PyPI-packaged version for deterministic, cache-friendly builds;
# provides the same `import clip` API as the upstream git install.
RUN --mount=type=cache,target=/root/.cache/pip \
    PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST} PIP_INDEX_URL=${PIP_INDEX_URL} \
    pip install openai-clip

RUN apt update && apt install -y curl && apt clean && rm -rf /var/lib/apt/lists/*

# Install useful JupyterLab extensions
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install jupyter-resource-usage

# Suppress the annoying announcements popup
RUN jupyter labextension disable "@jupyterlab/apputils-extension:announcements"

ADD docker/notebook/aperturedata /aperturedata

COPY docker/notebook/scripts/start.sh /start.sh
RUN  chmod 755 /start.sh

# Add Tini. Tini operates as a process subreaper for jupyter.
# This prevents kernel crashes.
# ENV TINI_VERSION v0.6.0
# ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/bin/tini
# RUN chmod +x /usr/bin/tini
# ENTRYPOINT ["/usr/bin/tini", "--"]
RUN --mount=type=cache,target=/root/.cache/pip \
    PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST} PIP_INDEX_URL=${PIP_INDEX_URL} \
    pip install -e "/aperturedata[dev]"
RUN echo "adb --install-completion" | bash

EXPOSE 8888
CMD ["/start.sh"]
