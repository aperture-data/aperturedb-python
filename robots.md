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
cp -r aperturedb pyproject.toml README.md docker/tests/aperturedata
mkdir -m 777 -p docker/tests/aperturedata/test/aperturedb

# 3. Copy test inputs and test scripts over
cp -r test/*.py test/*.sh test/input docker/tests/aperturedata/test

# 4. Build the test image locally (BuildKit enabled for faster layer caching)
DOCKER_BUILDKIT=1 docker build -t aperturedata/aperturedb-python-tests:latest -f docker/tests/Dockerfile .
```

> **Note on package versions**: The Dockerfile installs CLIP first (which pulls torch 2.12.0), then installs `.[dev]` extras (which pulls facenet-pytorch, downgrading torch to 2.2.2 and numpy to 1.26.4). The final installed versions are torch=2.2.2, torchvision=0.17.2, numpy=1.26.4, tensorflow=2.21.0, triton=2.2.0.

---

## 2. Running the Complete Test Suite

Once the test image is built, use the `run_test_container.sh` wrapper script located in the `test/` directory to coordinate the backend integration environment and run the test suite.

```bash
cd test
set -a && source .env && set +a
bash run_test_container.sh
```

This script:
1. Orchestrates spinning up `aperturedb`, `nginx`, `lenz` containers using `docker-compose.yml` on temporary networking bridges.
2. Starts two parallel test suites (HTTP and non-HTTP modes), each in its own isolated network.
3. Mounts the local `test/run_test.sh` into each test container so changes to it take effect without rebuilding the image.
4. Checks exit codes for successes or failures and cleans up.

Expected results with a clean environment: ~118 passed + 4 skipped for non-HTTP, ~75 passed + 4 skipped for HTTP. The 4 skipped tests require the `slow` mark which is not selected by default.

---

## 3. Running tests and bypassing AWS/GCP API requirements

A subset of the `pytest` tests interact with external objects, such as fetching blob configurations directly from cloud providers (AWS S3, Google Storage - GS). If your local environment or the automated agent lacks valid AWS/GCP credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `GCP_SERVICE_ACCOUNT_KEY`), you will hit authentication validation errors when resolving `storage.Client()` architectures or boto3 structures during execution.

To dynamically skip external storage tests, modify the `pytest` invocation inside `test/run_test.sh` before running. Because `run_test_container.sh` mounts this file directly into the test container at runtime, no image rebuild is needed — just edit the file and re-run.

Use this quick `sed` command to patch the exclusion:

```bash
cd test
# Modify the pytest invocation to ignore AWS/GCP specific loaders and remotely constrained mark configurations
sed -i 's/pytest --cov=aperturedb -m "\$FILTER"/pytest --cov=aperturedb -k "not test_S3ImageLoader and not test_GSImageLoader and not test_S3VideoLoader and not test_GSVideoLoader" -m "\$FILTER and not remote_credentials and not external_network"/' run_test.sh

# Then run the tests as usual
set -a && source .env && set +a
bash run_test_container.sh
```

By applying this change, any LLM/agent can successfully execute internal `aperturedb` connector + python-centric API tests without raising failures tied strictly to external cloud requirements.

---

## 4. Known Warnings (non-fatal)

These warnings appear during a successful local run and can be ignored:

- **`PytestUnknownMarkWarning`** for `slow`, `external_network`, `remote_credentials`, `tcp`, `http`, `dask` — these custom marks are used but not registered in `pyproject.toml`. Tests still run and filter correctly.
- **`CoverageWarning: No data was collected`** — the `--cov=aperturedb` flag collects coverage data inside the container, but the HTML report step (`coverage html`) cannot find it. This is a known limitation of the containerized test setup and does not indicate test failures.
- **`Port 8787 is already in use`** (Dask) — when two test suites run in parallel and both use Dask, the scheduler port conflicts. Dask auto-reassigns to an available port; no action needed.
- **Pydantic V2 deprecation** (`model_fields` on instance) — library internals, not caused by test code.
- **`numpy.fromstring` deprecation** in `aperturedb/transformers/clip.py` — use `np.frombuffer` instead; harmless for current numpy version.

---

## 5. Local vs CI Differences

| Aspect | Local | CI (GitHub Actions) |
|--------|-------|---------------------|
| **DB image** | `aperturedata/aperturedb-community:latest` | `aperturedata/aperturedb:dev` |
| **Lenz tag** | `latest` | `dev` |
| **Test filtering** | Skips `remote_credentials`, `external_network`, S3/GCS loaders | Runs all tests including cloud-dependent ones |
| **AWS/GCP credentials** | Not present — must patch `run_test.sh` | Present as GitHub secrets |
| **Log upload on failure** | Skipped (no AWS creds) | Uploads to S3 bucket `python-ci-runs` |
| **`BUILD_AUX_IMAGES`** | `true` (default in `ci.sh`) — builds notebook/coverage images | `false` — skips aux images to save time |
| **`TEST_PROTOCOL`** | `both` (default in `run_test_container.sh`) | `both` (explicit in `pr.yaml`) |
| **`RUNNER_NAME`** | `default` (from `test/.env`) | `${{ runner.name }}_pr` |
| **Docker cache** | Local layer cache only | BuildKit + `--cache-from` inline cache from registry |
| **Image push** | Never pushes | Pushes on merge to main (controlled by `NO_PUSH`) |
| **Kaggle credentials** | Dummy (`KAGGLE_username=ci`, `KAGGLE_key=dummy`) | Same dummy values |
| **Coverage HTML** | `coverage html` fails (no data in container path) | Same issue; coverage not collected meaningfully |
| **Concurrency** | N/A | New push to PR cancels in-progress run |
| **`run_test.sh` override** | Mounted as volume — edit `test/run_test.sh` locally (e.g. apply credentials filter from Section 3), no image rebuild needed | Also mounted as volume — the committed `test/run_test.sh` runs in CI. Do NOT commit the credentials filter; CI needs full test coverage |
