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
Parallel and Batch Loader for ApertureDB
:class:`~aperturedb.ParallelLoader.ParallelLoader` is further subclassed as:
   * :class:`~aperturedb.BBoxLoader.BBoxLoader`
   * :class:`~aperturedb.BlobLoader.BlobLoader`
   * :class:`~aperturedb.ConnectionLoader.ConnectionLoader`
   * :class:`~aperturedb.DescriptorLoader.DescriptorLoader`
   * :class:`~aperturedb.DescriptorSetLoader.DescriptorSetLoader`
   * :class:`~aperturedb.ImageLoader.ImageLoader`
   * :class:`~aperturedb.VideoLoader.VideoLoader`

Utils
-----
Miscelaneous :class:`~aperturedb.Utils.Utils`

.. note::
   For a complete list of functionality, refer to apertruredb > Submodules in the index

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   autoapi/aperturedb/index

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
