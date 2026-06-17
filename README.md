# ApertureDB Python SDK

[![PyPI version](https://badge.fury.io/py/aperturedb.svg)](https://badge.fury.io/py/aperturedb)
[![Python versions](https://img.shields.io/pypi/pyversions/aperturedb.svg)](https://pypi.org/project/aperturedb/)
[![License](https://img.shields.io/pypi/l/aperturedb.svg)](https://github.com/aperture-data/aperturedb-python/blob/main/LICENSE)
[![Build Status](https://github.com/aperture-data/aperturedb-python/actions/workflows/main.yml/badge.svg)](https://github.com/aperture-data/aperturedb-python/actions)

The **ApertureDB Python SDK** provides a seamless interface for interacting with [ApertureDB](https://docs.aperturedata.io/Introduction/WhatIsAperture), the purpose-built database for storing, managing, and querying visual data (images, videos) alongside metadata, embeddings, and annotations.

This SDK includes comprehensive utilities to efficiently ingest and retrieve data, manage graph-based metadata, perform similarity searches on embeddings, and seamlessly integrate with the broader ML/AI ecosystem.

## 🚀 Integrations & Capabilities

The SDK is designed for modern ML workflows and offers seamless integrations with (some require additional dependencies):
*   **Deep Learning Frameworks:** Seamless conversion of ApertureDB queries into `PyTorch` (`PyTorchDataset`) and `TensorFlow` (`TensorFlowDataset`) data loaders for immediate model training.
*   **Vector Search & Embeddings:** First-class support for storing and retrieving high-dimensional descriptors, including native embedding extraction utilizing `CLIP` (requires `openai-clip`) and `Facenet`.
*   **Distributed Data Processing:** Integration with `Dask` to handle parallelized data loading and large-scale query execution.
*   **Cloud Storage Integrations:** Easy handling of assets stored remotely using `Boto3` (AWS S3) and Google Cloud Storage.
*   **ML Croissant:** Native parsing and handling of datasets aligned with the ML Croissant metadata format.
*   **Knowledge Graphs:** Ability to execute and map SPARQL queries natively into the database (requires `rdflib`).

## 📦 Installation

To install the SDK in a standard virtual environment with all core integrations (such as PyTorch and TensorFlow support):

```bash
pip install "aperturedb[complete]"
```

To install just the lightweight core client without the heavy ML dependencies:

```bash
pip install aperturedb
```

## 📚 Documentation & Examples

*   [Getting Started Guide](https://docs.aperturedata.io/Setup/QuickStart)
*   [Python SDK Reference](https://docs.aperturedata.io/category/aperturedb-python-sdk)
*   [Simple Examples and Concepts](https://docs.aperturedata.io/category/start-with-basics)
*   [Advanced ML Usage Examples](https://docs.aperturedata.io/category/build-ml-examples)
*   [Sample Applications](https://docs.aperturedata.io/category/build-applications)

## 🛠 Development Setup

The recommended way to contribute is to clone this repository and perform an editable installation:

```bash
git clone https://github.com/aperture-data/aperturedb-python.git
cd aperturedb-python
pip install -e .[dev]
```

## 🧪 Running Tests

The tests are located inside the `test/` directory. They are designed to run within an isolated Linux container network composed of `aperturedb`, `lenz`, `nginx`, `ca`, and `webui`.

If you'd like to bring up the environment manually, ensure your working directory is `test/` and run:

```bash
docker compose up -d
```

### Development Environment Adjustments

To connect to the local test environment natively from your host, edit `test/dbinfo.py` to match the exposed ports (you can find the ephemeral ports by running `docker compose port lenz 55551` and `docker compose port nginx 443`):
```python
DB_TCP_HOST = 'localhost'
DB_REST_HOST = 'localhost'
DB_TCP_PORT  = 55551  # Replace with the mapped host port for lenz
DB_REST_PORT = 8443   # Replace with the mapped host port for nginx 443
VERIFY_HOSTNAME = False # Required when connecting to localhost to skip cert hostname validation
```

### Executing Tests

To run the entire test suite, export the necessary credentials and execute the test runner script:

```bash
export GCP_SERVICE_ACCOUNT_KEY=<content of a GCP SERVICE ACCOUNT JSON file>
bash run_test.sh
```

To run a specific test file natively via `pytest`:

```bash
PROJECT=aperturedata KAGGLE_username=ci KAGGLE_key=dummy coverage run -m pytest test_Session.py -v -s --log-cli-level=DEBUG
```

*(Note: The test suite assumes a Linux x86_64 environment. Adjustments may be required for macOS or Windows.)*

## ⚙️ Runtime Configuration Variables

The following environment variables can modify the runtime behavior of the SDK, which is particularly useful for debugging:

| Variable | Type | Description | Default Value |
| --- | --- | --- | --- |
| `ADB_DEBUGGABLE` | `boolean` | Allows the application to register a fault handler that dumps a trace when `SIGUSR1` is sent to the process. | *Not set* |
| `LOG_FILE_LEVEL` | `string` | The threshold for emitting log messages into the `error<timestamp>.log` file. | `WARN` |
| `LOG_CONSOLE_LEVEL`| `string` | The threshold for emitting log messages to `stdout`. | `ERROR` |
| `ADB_LOG_FILE` | `string` | Custom file path for the log output. | *Not set* |

## 🐞 Bug Reports & Contributing

*   **Bugs:** Please report any issues regarding functionality, documentation, or tests by creating a [GitHub Issue](https://github.com/aperture-data/aperturedb-python/issues).
*   **Contributing:** We welcome Pull Requests! When submitting a PR, include a brief description of the problem and the implemented fix.
*   **Coding Standards:** Our CI enforces coding style guidelines using `autopep8` and trailing whitespace checks. If a PR encounters failures, review the CI logs for details on the offending lines.
