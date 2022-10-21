# el-galyleo-client

The Python client for Galyleo Tables and JupyterLab.  This client is used to form Galyleo Tables, the basis of charting and dashboards with the Galyleo environment, and send them through the Jupyter communications channel to the browser to be plotted.  The library consists of several modules:
* galyleo_table.  Defines a Galyleo Dashboard Table and associated export and import routines.  Used to create a Galyleo Dashboard Table from any of a number of sources, and then generate an object that is suitable
for storage (as a JSON file).  A GalyleoTable is very similar to  a Google Visualization data table, and can be
converted to a Google Visualization Data Table on either the Python or the JavaScript side.
Convenience routines provided here to import data from pandas, and json format.
* galyleo_constants: Constants that are used, primarily by galyleo_table
* galyleo_exceptions: Exceptions raised by the module.  These notably include exceptions raised by galyleo_jupyter_client when a table is too large to be sent, and by galyleo_table when a table's schema and data don't match
* galyleo_jupyter_client: A clien that actually sends data to JupyterLab
