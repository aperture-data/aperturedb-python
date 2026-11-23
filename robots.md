# ApertureDB Python Testing Guide

This guide is designed for automated agents and LLMs to quickly understand how to build and run the test suite for `py-aperturedb` locally.

## Prerequisites
The integration tests rely on local `aperturedb` + `lenz` containers orchestrated by Docker Compose. The environment runs a client container to execute the `pytest` suite against these spun-up backends.

---

## 1. Building the Test Environment Image

To run the tests locally, you first need to build the `aperturedata/aperturedb-python-tests:latest` Docker image. From the workspace root, populate the expected context directory and execute `docker build`:

```bash
# 1. Provide the expected context data for the test Dockerfile
mkdir -p test/aperturedb/logs/runner_state
mkdir -p docker/tests/aperturedata

# 2. Copy the Python source and configurations over
cp -r aperturedb examples pyproject.toml README.md docker/tests/aperturedata
mkdir -m 777 -p docker/tests/aperturedata/test/aperturedb

# 3. Copy test inputs and test scripts over
cp -r test/*.py test/*.sh test/input docker/tests/aperturedata/test

# 4. Build the test image locally (BuildKit enabled for faster layer caching)
DOCKER_BUILDKIT=1 docker build -t aperturedata/aperturedb-python-tests:latest -f docker/tests/Dockerfile .
```

> **Note on package versions**: The Dockerfile installs CLIP first (which pulls torch 2.12.0), then installs `.[dev]` extras (which pulls facenet-pytorch, downgrading torch to 2.2.2 and numpy to 1.26.4). The final installed versions are torch=2.2.2, torchvision=0.17.2, numpy=1.26.4, tensorflow=2.21.0, triton=2.2.0.

---

## 2. Running the integration suite

From the repository root, run the test image against the infra-promoted
Compose stack. The test container exercises native TLS and HTTPS on the same
isolated network and records the database image digest and source revision.

```bash
RUNNER_NAME=sdk_local ./test/run_compatibility_container.sh
```

The local runner defaults to the same Compose tag as CI. Set `ADB_COMPOSE` to
test a different published artifact locally. See the [SDK validation
guide](docs/SDK1Validation.md) for report paths.

---

## 3. Known Warnings (non-fatal)

These warnings appear during a successful local run and can be ignored:

- **`Port 8787 is already in use`** (Dask) — when two test suites run in parallel and both use Dask, the scheduler port conflicts. Dask auto-reassigns to an available port; no action needed.
- **Pydantic V2 deprecation** (`model_fields` on instance) — library internals, not caused by test code.
- **`numpy.fromstring` deprecation** in `aperturedb/transformers/clip.py` — use `np.frombuffer` instead; harmless for current numpy version.

---

## 4. Local and CI runs

| Setting | Local container runner | CI |
|---------|------------------------|----|
| Compose tag | Defaults to `latest` | Pull requests, develop, main and nightly use `latest`. |
| Test image | Rebuild after editing the SDK or Python tests. | CI builds from the checkout. The GPU PR job uses the PyTorch GPU image. |
| Stack name | Set a unique `RUNNER_NAME` per concurrent run. | The workflow sets a name for each job. |
| Coverage | Open `test/output/index.html` after a direct runner invocation. | Jobs that build the coverage image copy the report into it. |
