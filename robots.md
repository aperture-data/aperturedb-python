# ApertureDB Python Testing Guide

This guide is designed for automated agents and LLMs to quickly understand how to build and run the test suite for `py-aperturedb` locally.

## Prerequisites
The integration tests rely on local `aperturedb` + `lenz` containers orchestrated by Docker Compose. The environment runs a client container to execute the `pytest` suite against these spun-up backends.

---

## 1. Building the Test Environment Image

To run the tests locally, you first need to build the `aperturedb-python-tests:latest` Docker image. From the workspace root (`/home/remis/src/ad/py-aperturedb`), populate the expected context directory and execute `docker build`:

```bash
# 1. Provide the expected context data for the test Dockerfile
mkdir -p test/aperturedb/logs/runner_state
mkdir -p docker/tests/aperturedata

# 2. Copy the Python source and configurations over
cp -r aperturedb pyproject.toml README.md docker/tests/aperturedata
mkdir -m 777 -p docker/tests/aperturedata/test/aperturedb

# 3. Copy test inputs and test scripts over
cp -r test/*.py test/*.sh test/input docker/tests/aperturedata/test

# 4. Build the test image locally
docker build -t aperturedata/aperturedb-python-tests:latest -f docker/tests/Dockerfile .
```

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
2. Starts two parallel test suites (HTTP and non-HTTP modes).
3. Checks exit codes for successes or failures and cleans up.

---

## 3. Running tests and bypassing AWS/GCP API requirements

A subset of the `pytest` tests interact with external objects, such as fetching blob configurations directly from cloud providers (AWS S3, Google Storage - GS). If your local environment or the automated agent lacks valid AWS/GCP credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `GCP_SERVICE_ACCOUNT_KEY`), you will hit authentication validation errors when resolving `storage.Client()` architectures or boto3 structures during execution.

To dynamically skip external storage tests, you should inform `pytest` running inside the Docker sequence to skip functions interacting with S3 and Google Storage.

Before running the test script, manually modify the `pytest` invocation inside `test/run_test.sh` to explicitly add `-k "not S3 and not GS"` to exclude any specific loading functions targeting S3 or GS, along with keeping away tests appropriately marked with `@pytest.mark.remote_credentials`.

Use this quick `sed` command to patch the exclusion:

```bash
cd test
# Modify the pytest invocation to ignore AWS/GCP specific loaders and remotely constrained mark configurations
sed -i 's/pytest -m "$FILTER"/pytest -k "not test_S3ImageLoader and not test_GSImageLoader and not test_S3VideoLoader and not test_GSVideoLoader" -m "$FILTER and not remote_credentials and not external_network"/' run_test.sh

# Then run the tests as usual
set -a && source .env && set +a
bash run_test_container.sh
```

By applying this change, any LLM/agent can successfully execute internal `aperturedb` connector + python-centric API tests without raising failures tied strictly to external cloud requirements.
