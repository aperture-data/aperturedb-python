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

Build the current SDK test image and run the integration suite against a single Compose stack over native TLS and HTTPS.

```bash
RUNNER_NAME=sdk_local NO_PUSH=true RUN_TESTS=true BUILD_AUX_IMAGES=false bash ci.sh
```

To reuse the test image, run `RUNNER_NAME=sdk_local ./test/run_compatibility_container.sh`. The release gate and report paths are documented in [SDK validation](docs/SDK1Validation.md).

### Running Tests Natively (Focused Verification)
If you only need to run a single test file (e.g. `test_Session.py`) and don't want to run the full containerized suite:
1. Bring up the environment in the background:
   ```bash
   cd test
   ./stack.sh up local
   ```
2. Update `test/dbinfo.py`: both hosts `localhost`, `DB_TCP_PORT = 55555`, `DB_REST_PORT = 443` (or the `ADB_TCP_PORT` and `HTTPS_PORT` you set), and `CA_CERT = 'aperturedb/local/ca/ca.crt'`. Run `./stack.sh down local` when done.
3. Execute the single test:
   ```bash
   PROJECT=aperturedata KAGGLE_username=ci KAGGLE_key=dummy python3 -m pytest test_Session.py -v -s --log-cli-level=DEBUG
   ```

### Known Test Warnings (Non-fatal)
- `Port 8787 is already in use`: Dask parallel run conflict, Dask auto-reassigns port.
Ignore these warnings during a successful test run.

## Workflow Rules
- For integration failures, inspect the container output and the JUnit report in `qualification/sdk-compat/junit.xml`. Rebuild the SDK test image after changing the SDK or Python tests.
- When generating ML placeholders or dummy assets, keep them minimal. The test environment has built-in dummy generation via `generateInput.py` and `download_images.py`.
