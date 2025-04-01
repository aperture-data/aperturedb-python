# ApertureDB Client Python Module

This is the Python SDK for building applications with [ApertureDB](https://docs.aperturedata.io/Introduction/WhatIsAperture).

This comprises of utilities to get sata in and out of ApertureDB in an optimal manner.
A quick [getting started guide](https://docs.aperturedata.io/HowToGuides/start/Setup) is useful to start building with this SDK.
For more concrete examples, please refer to:
* [Simple examples and concepts](https://docs.aperturedata.io/category/simple-usage-examples)
* [Advanced usage examples](https://docs.aperturedata.io/category/advanced-usage-examples)

# Installing in a custom virtual enviroment
```bash
pip install aperturedb[complete]
```

or an installation with only the core part of the SDK
```bash
pip install aperturedb
```

A complete [reference](https://docs.aperturedata.io/category/aperturedb-python-sdk) of this SDK is available on the official [ApertureDB Documentation](https://docs.aperturedata.io)


# Development setup
The recommended way is to clone this repo, and do an editable install as follows:
```bash
git clone https://github.com/aperture-data/aperturedb-python.git
cd aperturedb-python
pip install -e .[dev]
```


# Running tests
The tests are inside the `test` dir. Currently these get run in Linux container. Refer to `docker/tests` and `test/run_test_container` for details. Following explanation assumes that the current working directory is `test`.

The tests bring up a set of components in an isolated network, namely:
- aperturedb-community
- lenz
- nginx
- ca (for initial provisioning of certificates)
- webui


To connect to this setup, the ports are exposed to the host as follows:
- 55556 for TCP connection to aperturedb (via lenz).
- 8087 for HTTP connection to aperturedb (via nginx).



This can be done manually as:
```bash
docker compose up -d
```

## Changes to run the tests in development environment.
Edit the file `test/dbinfo.py` to loook like the following.
- DB_TCP_HOST = `localhost`
- DB_REST_HOST = `localhost`
- DB_TCP_PORT  = `55556`
- DB_REST_PORT = `8087`


All the tests can be run with:

```bash
export GCP_SERVICE_ACCOUNT_KEY=<content of a GCP SERVICE ACCOUNT JSON file>
bash run_test.sh
```

Running specific tests can be accomplished by invoking it with pytest as follows:

```bash
PROJECT=aperturedata KAGGLE_username=ci KAGGLE_key=dummy coverage run -m pytest test_Session.py -v -s --log-cli-level=DEBUG
```

**NOTE:The running environment is assumed to be Linux x86_64. There might be certain changes required for them to be run on MacOS or Windows python environments.**

## Certain Environment variables that affect the runtime beaviour of the SDK.

These can be used as debugging aids.

| Variable | type | Comments | Default value |
| --- | --- | --- | --- |
|ADB_DEBUGGABLE | boolean | allows the application to register a fault handler that dumps a trace when SIGUSR1 is sent to the process | not set |
|LOG_FILE_LEVEL |  <a href="https://docs.python.org/3/library/logging.html#logging-levels">log levels</a> | The threshold for emitting log messages into the error<timestamp>.log file | WARN |
|LOG_CONSOLE_LEVEL | <a href="https://docs.python.org/3/library/logging.html#logging-levels">log levels</a> | The threshold for emitting log messages into stdout | ERROR |
|ADB_LOG_FILE | string | custom file path for the LOG file | not set|


# Reporting bugs
Any error in the functionality / documentation / tests maybe reported by creating a
[github issue](https://github.com/aperture-data/aperturedb-python/issues).

# Development guidelines
For inclusion of any features, a PR may be created with a patch,
and a brief description of the problem and the fix.
The CI enforces a coding style guideline with autopep8 and
a script to detect trailing white spaces.

If a PR encounters failures, the log will describe the location of
the offending line with a description of the problem.
