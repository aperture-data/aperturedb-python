# ApertureDB Python SDK Agent Instructions

This guide provides high-signal context for AI agents working in this repository. 

## Architecture & Entrypoints
- **Type**: Python SDK for ApertureDB.
- **Entrypoints**: The core package is in the `aperturedb/` directory. 
- **CLI**: Provides the `adb` CLI tool, mapped to `aperturedb.cli.adb:app`.
- **Packaging**: Uses `setuptools` with configurations defined in `pyproject.toml`.

## Setup & Code Style
- **Formatting**: CI strictly enforces `autopep8` and trailing whitespace checks. If you modify code, ensure it aligns with these standards. You can use `pre-commit run --all-files` if pre-commit is installed, or rely on `autopep8`.
- **Dependencies**: Extra dependencies are organized in `pyproject.toml` (e.g. `[complete]`, `[notebook]`, `[dev]`). When running locally, a standard dev installation is `pip install -e '.[dev]'`.

## Testing Quirks & Execution
The test suite is located in `test/` and heavily relies on a local isolated container environment. It is NOT a standard simple pytest suite.

### Running Containerized Tests
1. The full suite orchestrates `aperturedb`, `lenz`, and `nginx` containers using Docker Compose.
2. **Crucial for Agents**: To run tests without AWS/GCP credentials (which agents typically lack), you **must** use `SKIP_SLOW_TESTS=true`.
3. To build the test image and run the full suite:
   ```bash
   # Build the image using the CI script
   source version.sh && NO_PUSH=true RUN_TESTS=true BUILD_AUX_IMAGES=false bash ci.sh
   # OR build manually if needed:
   # mkdir -p docker/tests/aperturedata && cp -r aperturedb pyproject.toml README.md docker/tests/aperturedata && mkdir -p docker/tests/aperturedata/test/aperturedb && cp -r test/*.py test/*.sh test/input docker/tests/aperturedata/test && docker build -t aperturedata/aperturedb-python-tests:latest -f docker/tests/Dockerfile .
   ```
4. If the test image is already built, you can run the suite directly:
   ```bash
   cd test
   set -a && source .env && set +a
   SKIP_SLOW_TESTS=true bash run_test_container.sh
   ```

### Running Tests Natively (Focused Verification)
If you only need to run a single test file (e.g. `test_Session.py`) and don't want to run the full containerized suite:
1. Bring up the environment in the background:
   ```bash
   cd test
   docker compose up -d
   ```
2. Update `test/dbinfo.py` to map the local exposed ports (find them via `docker compose port lenz 55551` and `docker compose port nginx 443`).
3. Execute the single test:
   ```bash
   PROJECT=aperturedata KAGGLE_username=ci KAGGLE_key=dummy python3 -m pytest test_Session.py -v -s --log-cli-level=DEBUG
   ```

### Known Test Warnings (Non-fatal)
- `PytestUnknownMarkWarning`: For `slow`, `external_network`, `remote_credentials`, `tcp`, `http`, `dask`. Tests still filter correctly.
- `CoverageWarning: No data was collected`: Known limitation of the containerized setup.
- `Port 8787 is already in use`: Dask parallel run conflict, Dask auto-reassigns port.
Ignore these warnings during a successful test run.

## Workflow Rules
- Prefer executable sources of truth: if tests fail, look at the container output and `.log` files generated in `test/output/client/`.
- `run_test.sh` is mounted into the test container as a volume; edits to it apply locally without a container rebuild.
- When generating ML placeholders or dummy assets, keep them minimal. The test environment has built-in dummy generation via `generateInput.py` and `download_images.py`.
