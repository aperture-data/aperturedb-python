# ApertureDB Python SDK

[![PyPI version](https://badge.fury.io/py/aperturedb.svg)](https://badge.fury.io/py/aperturedb)
[![Python versions](https://img.shields.io/pypi/pyversions/aperturedb.svg)](https://pypi.org/project/aperturedb/)
[![License](https://img.shields.io/pypi/l/aperturedb.svg)](https://github.com/aperture-data/aperturedb-python/blob/main/LICENSE)
[![Build Status](https://github.com/aperture-data/aperturedb-python/actions/workflows/main.yml/badge.svg)](https://github.com/aperture-data/aperturedb-python/actions)

The **ApertureDB Python SDK** provides a seamless interface for interacting with [ApertureDB](https://docs.aperturedata.io/Introduction/WhatIsAperture), the purpose-built database for storing, managing, and querying visual data (images, videos) alongside metadata, embeddings, and annotations.

This SDK includes comprehensive utilities to efficiently ingest and retrieve data, manage graph-based metadata, perform similarity searches on embeddings, and seamlessly integrate with the broader ML/AI ecosystem.

## Connecting and querying with SDK 1.0.3

The default `Connector` uses native TCP with TLS and requires an endpoint that
accepts hello version 2. For HTTPS, use `ConnectorRest` or a connection profile
with `use_rest=True` and the HTTP gateway port. Use `query()` for a buffered
response and `query_stream()` to consume blobs in chunks. The client buffers HTTP
responses that carry blobs as base64 in JSON. If a transport failure
leaves a write's outcome uncertain, check its effects before retrying.

SDK helpers and loaders select commands for the connected server. Custom query
generators must use its command language. For typed classes, supply application
class names on inserts and use native vector indexes. `AddConnection` takes a
connection name and source and destination references. Grouped connection
selectors put class names under `class` and endpoint types under `target`.
[Connection, query and streaming examples](docs/SDK1Migration.md)

## 🚀 Integrations & Capabilities

The SDK is designed for modern ML workflows and offers seamless integrations with (some require additional dependencies):
*   **Deep Learning Frameworks:** Seamless conversion of ApertureDB queries into `PyTorch` (`PyTorchDataset.ApertureDBDataset`) and `TensorFlow` (`ApertureDBTensorFlowDataset`) data loaders for immediate model training.
*   **Vector Search & Embeddings:** First-class support for storing and retrieving high-dimensional descriptors, including native embedding extraction utilizing `CLIP` (requires `openai-clip`) and `Facenet`.
*   **Distributed Data Processing:** Integration with `Dask` to handle parallelized data loading and large-scale query execution.
*   **Cloud Storage Integrations:** Easy handling of assets stored remotely using `Boto3` (AWS S3) and Google Cloud Storage.
*   **ML Croissant:** Native parsing and handling of datasets aligned with the ML Croissant metadata format (requires `mlcroissant`).
*   **Knowledge Graphs:** Ability to execute and map SPARQL queries natively into the database (requires `rdflib`).

## 📦 Installation

To install the SDK in a standard virtual environment with all core integrations (such as PyTorch and TensorFlow support):

```bash
pip install "aperturedb[complete]"
```

To install just the base client without optional deep learning extras:

```bash
pip install aperturedb
```

CI builds can install the promoted wheel from Docker Hub with ORAS. SDK main
publishes the versioned artifact first (for example, `1.0.3`). That tag is
available as soon as the SDK release completes. Infra assigns `latest` to the
wheel and publishes it to PyPI when the component reaches main.

After the first wheel promotion, Dockerfiles that used
`FROM aperturedata/aperturedb-python:latest` need a Python base image. Pull the
wheel with ORAS and install it into that environment.

```bash
wheel_dir=$(mktemp -d)
oras pull registry-1.docker.io/aperturedata/aperturedb-python:latest -o "$wheel_dir"
python -m pip install "$wheel_dir"/aperturedb-*-py3-none-any.whl
```

## Authentication and shared sessions

The SDK signs in when you first run a query and renews the session as needed.
If the server rejects a session before running a query, the SDK renews it and
retries that query once. When the refresh token is no longer accepted, it signs
in again using the password or personal token configured on the connector.
If those credentials are also rejected, the query raises an exception.

Use a separate `Connector` for each thread, created with `db.clone()`. Clones
share a session, so concurrent requests reuse the same login and coordinate
session renewal. Dask tasks create independent sessions using the same configured
credentials, allowing each process to renew its own session.

On servers that apply security changes immediately, existing connectors observe
updated permissions on subsequent queries. Removing a user's access or revoking
their configured credentials can cause those queries to fail. Queries sent to
replicas may briefly observe earlier permissions while replication catches up.
Permission errors are returned to the caller. Explicit `Authenticate` and
`RefreshToken` queries also return their errors without automatic session recovery.

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
pip install -e '.[dev]'
```

## 🧪 Running Tests

The tests are located inside the `test/` directory. They run against an ApertureDB stack (`aperturedb`, `lenz`, `http-proxy` and `webui`) started from the published Docker Compose artifact `aperturedata/aperturedb-compose-single-node`. `ADB_COMPOSE` selects the artifact; the default is `dev`, the newest athena main release, which infra stages in its develop environment.

If you'd like to bring up the environment manually, ensure your working directory is `test/` and run:

```bash
./stack.sh up local
```

It publishes the native protocol on `localhost:55555` and HTTPS on `localhost:443`; set `ADB_TCP_PORT` and `HTTPS_PORT` to use other ports. `./stack.sh down local` stops it and deletes its files.

### Development Environment Adjustments

To connect to the local test environment natively from your host, edit `test/dbinfo.py` to match the published ports and the stack's CA:
```python
DB_TCP_HOST = 'localhost'
DB_REST_HOST = 'localhost'
DB_TCP_PORT  = 55555  # ADB_TCP_PORT
DB_REST_PORT = 443    # HTTPS_PORT
CA_CERT      = 'aperturedb/local/ca/ca.crt'
```

### Executing Tests

Run the release integration suite from the repository root with a test image
built from the current checkout.

```bash
RUNNER_NAME=sdk_local ./test/run_compatibility_container.sh
```

The suite exercises native TLS and HTTPS on one server. Build instructions are
in the [testing guide](robots.md).

To run a specific test file against the local stack, use the test directory.

```bash
cd test
PROJECT=aperturedata KAGGLE_username=ci KAGGLE_key=dummy coverage run -m pytest test_Session.py -v -s --log-cli-level=DEBUG
```

*(Note: The test suite assumes a Linux x86_64 environment. Adjustments may be required for macOS or Windows.)*

## ⚙️ Runtime Configuration Variables

The following environment variables can modify the runtime behavior of the SDK, which is particularly useful for debugging:

| Variable | Type | Description | Default Value |
| --- | --- | --- | --- |
| `ADB_DEBUGGABLE` | `flag` | If set (to any value), allows the application to register a fault handler that dumps a trace when `SIGUSR1` is sent to the process. | *Not set* |
| `LOG_FILE_LEVEL` | `string` | The threshold for emitting log messages into the `error<timestamp>.log` file. | `WARN` |
| `LOG_CONSOLE_LEVEL`| `string` | The threshold for emitting log messages to `stdout`. | `ERROR` |
| `ADB_LOG_FILE` | `string` | Custom file path for the log output. | *Not set* |

## 🐞 Bug Reports & Contributing

*   **Bugs:** Please report any issues regarding functionality, documentation, or tests by creating a [GitHub Issue](https://github.com/aperture-data/aperturedb-python/issues).
*   **Contributing:** We welcome Pull Requests! When submitting a PR, include a brief description of the problem and the implemented fix.
*   **Coding Standards:** Our CI enforces coding style guidelines using `autopep8` and trailing whitespace checks. If a PR encounters failures, review the CI logs for details on the offending lines.
