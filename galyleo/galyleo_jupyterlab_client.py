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

from ipykernel.comm import Comm
from galyleo.galyleo_table import GalyleoTable

'''
The Dashboard Client.  This is the client which sends the tables to the dashboard
and handles requests coming from the dashboard for tables.
'''

class GalyleoClient:
  def __init__(self):
    self._comm_ = Comm(target_name='galyleo_data', data={'foo': 1})

#
# The routine to send a GalyleoTable to the dashboard, optionally specifying a specific 
# dashboard to send the data to.  If None is specified, 
# 

  def send_data_to_dashboard(self, galyleo_table, dashboard_name:str = None)->None:
    # Very simple. Convert the table to a dictionary and send it to the dashboard
    # and wrap it in a payload to send to the dashboard
    table_record = galyleo_table.as_dictionary()
    if (dashboard_name):
      table_record["dashboard"] = dashboard_name
    self._comm_.send(table_record)
