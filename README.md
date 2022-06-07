# ApertureDB Client Python Module

This is the python client module for ApertureDB.

It provides a connector to AperetureDB instances using
the open source connector for [VDMS](https://github.com/IntelLabs/vdms).

It also implements an Object-Mapper API to interact with
elements in ApertureDB at the object level.

* Utils.py provides helper methods to retrieve information about the db.
* Images.py provides the Object-Mapper for image related objetcs (images, bounding boxes, etc)
* NotebookHelpers.py provides helpers to show images/bounding boxes on Jupyter Notebooks

For more information, visit https://python.docs.aperturedata.io

# Running tests.
The tests are inside the test dir.

All the tests can be run with:

``bash run_test.sh``

Running specefic tests can be accomplished by invoking it with pytest as follows:

``python -m pytest test_Session.py -v --log-cli-level=DEBUG``

# Reporting bugs.
Any error in the functionality / documentation / tests maybe reported by creating a
[github issue](https://github.com/aperture-data/aperturedb-python/issues).

# Development guidelines.
For inclusion of any features, a PR may be created with a patch,
and a brief description of the problem and the fix.
The CI enforces a coding style guideline with autopep8 and
a script to detect trailing white spaces.

In case a PR encounters failures, the log would describe the location of
the offending line with a description of the problem.
