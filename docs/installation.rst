Installation
============

This guide covers how to install datashard for different use cases.

Prerequisites
-------------

- Python 3.7 or higher
- pip package manager

Installing from PyPI
--------------------

The easiest way to install datashard is using pip:

.. code-block:: bash

   pip install datashard

Installing from Source
----------------------

If you want to install from the source code:

.. code-block:: bash

   git clone https://github.com/rodmena-limited/datashard.git
   cd datashard
   pip install .

Development Installation
--------------------------

For development purposes, you can install in editable mode with development dependencies:

.. code-block:: bash

   git clone https://github.com/rodmena-limited/datashard.git
   cd datashard
   pip install -e ".[dev]"

Verification
------------

To verify that datashard is installed correctly:

.. code-block:: python

   import datashard
   print(datashard.__version__)

If this runs without error, datashard is properly installed.