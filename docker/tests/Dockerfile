# Lightweight test image: does NOT inherit from aperturedb-notebook:dependencies,
# which compiles OpenCV from source (~15-20 min on a cold build).  Tests use
# opencv-python-headless from PyPI (already a runtime dep in pyproject.toml).
FROM python:3.10
ARG PIP_INDEX_URL=https://pypi.org/simple
ARG PIP_TRUSTED_HOST=pypi.org

# System packages needed by the test suite:
#   ffmpeg + codec libs  – video loading / processing tests
#   libgl1               – OpenCV headless still links against libGL on some codecs
#   libfuse-dev + fuse   – fuse-python (listed in [dev] extras)
#   git                  – any remaining git-based pip installs
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg \
        libavcodec-extra \
        libgl1 \
        libfuse-dev \
        fuse \
        git \
    && rm -rf /var/lib/apt/lists/*

# --- Stable tools layer (almost never invalidated) ---
# awscli  : upload logs to S3 on test failure
# openai-clip : PyPI-packaged CLIP model (deterministic version, no git-clone
#               on every build).  Provides `import clip` with the same API.
RUN --mount=type=cache,target=/root/.cache/pip \
    PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST} PIP_INDEX_URL=${PIP_INDEX_URL} \
    pip install awscli openai-clip

# --- Dependency layer (cached until pyproject.toml changes) ---
# Copy only the package spec before the code so the heavy pip install step
# (torch, tensorflow, torchvision, facenet-pytorch, …) is a separate cached
# layer that survives code-only commits.
COPY docker/tests/aperturedata/pyproject.toml /tmp/build/pyproject.toml
COPY docker/tests/aperturedata/README.md      /tmp/build/README.md
# Minimal stub so setuptools can resolve the editable install path and read
# the dynamic version attribute without importing the real package.
RUN mkdir -p /tmp/build/aperturedb && \
    printf '__version__ = "0.0.0"\n' > /tmp/build/aperturedb/__init__.py
RUN --mount=type=cache,target=/root/.cache/pip \
    PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST} PIP_INDEX_URL=${PIP_INDEX_URL} \
    pip install -e "/tmp/build[dev]"

# --- Code layer (invalidated on every commit) ---
# After the ADD, re-register the editable install at the real path.
# --no-deps prevents re-downloading the heavy packages already installed above.
RUN mkdir /aperturedata
ADD docker/tests/aperturedata /aperturedata
RUN --mount=type=cache,target=/root/.cache/pip \
    PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST} PIP_INDEX_URL=${PIP_INDEX_URL} \
    pip install -e "/aperturedata[dev]" --no-deps

COPY docker/tests/scripts/start.sh /start.sh
RUN  chmod 755 /start.sh
CMD ["/start.sh"]
