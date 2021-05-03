# BSD 3-Clause License

# Copyright (c) 2019-2021, engageLively
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:

# 1. Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.

# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.

# 3. Neither the name of the copyright holder nor the names of its
#    contributors may be used to endorse or promote products derived from
#    this software without specific prior written permission.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

"""
The Python client for Galyleo Tables and JupyterLab.  This client is used to form Galyleo Tables, the basis of charting and dashboards with the Galyleo environment, and send them through the Jupyter communications channel to the browser to be plotted.  The library consists of several modules:
1. galyleo_table.  Defines a Galyleo Dashboard Table and associated export and import routines.  Used to create a Galyleo Dashboard Table from any of a number of sources, and then generate an object that is suitable
for storage (as a JSON file).  A GalyleoTable is very similar to  a Google Visualization data table, and can be
converted to a Google Visualization Data Table on either the Python or the JavaScript side.
Convenience routines provided here to import data from pandas, and json format.
2. galyleo_constants: Constants that are used, primarily by galyleo_table
3. galyleo_exceptions: Exceptions raised by the module.  These notably include exceptions raised by galyleo_jupyter_client when a table is too large to be sent, and by galyleo_table when a table's schema and data don't match
4. galyleo_jupyter_client: A clien that actually sends data to JupyterLab
"""
name = "galyleo"