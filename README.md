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

A complete [reference](https://docs.aperturedata.io/category/aperturedb-python-sdk) of this SDK is available on the offical [ApertureDB Documentation](https://docs.aperturedata.io)


# Development setup
The recommended way is to clone this repo, and do an editable install as follows:
```bash
git clone https://github.com/aperture-data/aperturedb-python.git
cd aperturedb-python
pip install -e .[dev]
```


# Running tests
The tests are inside the `test` dir.

All the tests can be run with:

```bash
export GCP_SERVICE_ACCOUNT_KEY=<content of a GCP SERVICE ACCOUNT JSON file>
bash run_test.sh
```

Running specific tests can be accomplished by invoking it with pytest as follows:

```bash
cd test && docker compose up -d && PROJECT=aperturedata KAGGLE_username=ci KAGGLE_key=dummy coverage run -m pytest test_Session.py -v --log-cli-level=DEBUG
```

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
