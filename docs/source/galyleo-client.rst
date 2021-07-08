.. image :: images/galyleo-logo.png
   :width: 100


The Galyleo Python Client
=========================

The Galyleo Python client is a module designed to convert Python structures into Galyleo Tables, and send them to dashboards for use with the Galyleo editor.  It consists of four components:

- galyleo.galyleo_table: classes and methods to create GalyleoTables, convert Python data structures into them, and produce and read JSON versions of the tables.
- galyleo.galyleo_jupyterlab_client: classes and methods to send Galyleo Tables to Galyleo dashboards runniung under JupyterLab clients
- galyleo.galyleo_constants: Symbolic constants used by these packages and the code which uses them
- galyleo.galyleo_exceptions; Exceptions thrown by the package
  
Installation
------------

The galyleo module can be installed using ``pip``:
::

  pip install --extra-index-url https://pypi.engagelively.com galyleo  

When the module is more thoroughly tested, it will be put on the standard pypi servers.

License
-------

``galyleo`` is released under a standard BSD 3-Clause licence by engageLively

Galyleo Table
--------------

.. automodule:: galyleo.galyleo_table
   :members:


JupyterLab Client
-----------------

.. automodule:: galyleo.galyleo_jupyterlab_client
   :members:

Galyleo Exceptions
------------------

.. automodule:: galyleo.galyleo_exceptions
   :members:

Galyleo Constants
------------------

.. automodule:: galyleo.galyleo_constants
   :members:


