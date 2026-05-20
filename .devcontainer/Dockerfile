# Pull base image.
FROM aperturedata/aperturedb-notebook:dependencies

RUN python -m pip install --no-cache-dir \
    awscli==1.45.11 \
    "git+https://github.com/openai/CLIP.git@d05afc436d78f1c48dc0dbf8e5980a9d471f35f6"
