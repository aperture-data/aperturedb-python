.. aperturedb-python documentation master file, created by
   sphinx-quickstart on Tue Mar 15 11:42:50 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

ApertureDB Python Package Documentation.
==================================
aperturedb-python is a helper package that facilitates interaction with an instance of aperturedb.

Installation
------------
``pip install aperturedb``.

On a high level, the main modules and interfaces provided by the pacakge are as follows.

Connector
---------
:class:`~aperturedb.Connector.Connector` is a class to facilitate connections with an instance of aperturedb

Loader (ParallelLoader)
-----------------------
:class:`~aperturedb.ParallelLoader.ParallelLoader`
is a mechanism to ingest contents represented by one of the Data Classes into apertruredb.

It takes as input an object instantiated from one of the following classes.
   * :class:`~aperturedb.BBoxDataCSV.BBoxDataCSV`
   * :class:`~aperturedb.BlobDataCSV.BlobDataCSV`
   * :class:`~aperturedb.ConnectionDataCSV.ConnectionDataCSV`
   * :class:`~aperturedb.DescriptorDataCSV.DescriptorDataCSV`
   * :class:`~aperturedb.DescriptorSetDataCSV.DescriptorSetDataCSV`
   * :class:`~aperturedb.ImageDataCSV.ImageDataCSV`
   * :class:`~aperturedb.VideoDataCSV.VideoDataCSV`
   * A class derived from :class:`~aperturedb.PytorchData.PytorchData`
   * A class derived from :class:`~apertruredb.KaggleData.KaggleData`

Utils
-----
:class:`~aperturedb.Utils.Utils`

Miscelaneous  utilities consists of various helper
functions that are often used by applications built using apertruredb.

.. note::
   For a complete list of functionality, refer to apertruredb > Submodules in the index.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   autoapi/aperturedb/index
   examples/loaders
   examples/pytorch_classification
   examples/similarity_search

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
