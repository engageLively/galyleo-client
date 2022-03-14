# BSD 3-Clause License

# Copyright (c) 2019-2022, engageLively
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

import sys
import typing_extensions
sys.path.append('..')
from galyleo.galyleo_server_framework import GalyleoServerFramework
from galyleo.galyleo_table_server import GalyleoDataServer
from galyleo.galyleo_constants import GALYLEO_NUMBER, GALYLEO_STRING 
import pandas as pd
from flask import Flask

class ServeFromPandas:
    def __init__(self, spec):
        file_name = spec[0]
        galyleo_types = spec[1]
        self.dataframe  = pd.read_csv(file_name)
        columns = self.dataframe.columns.to_list()
        schema = [{"name": columns[i], "type": galyleo_types[i]} for i in range(len(columns))]
        self.server = GalyleoDataServer(schema, self.get_rows)
        self.columns = columns
        self.types = galyleo_types

    def get_rows(self):
        return self.dataframe.to_numpy().tolist()

app = Flask(__name__)

framework = GalyleoServerFramework(app)

tables = {
    'electoral_college': ('electoral_college.csv', [GALYLEO_NUMBER, GALYLEO_NUMBER, GALYLEO_NUMBER, GALYLEO_NUMBER]),
    'nationwide_vote': ('nationwide_vote.csv', [GALYLEO_NUMBER, GALYLEO_STRING, GALYLEO_NUMBER]),
    'presidential_vote': ('presidential_vote.csv', [GALYLEO_NUMBER, GALYLEO_STRING, GALYLEO_STRING, GALYLEO_STRING, GALYLEO_NUMBER, GALYLEO_NUMBER]),
    'presidential_margins': ('presidential_margins.csv', [GALYLEO_STRING, GALYLEO_NUMBER, GALYLEO_NUMBER]),
    'presidential_vote_history': ('presidential_vote_history.csv', [GALYLEO_STRING, GALYLEO_NUMBER, GALYLEO_NUMBER, GALYLEO_NUMBER, GALYLEO_NUMBER, GALYLEO_NUMBER, GALYLEO_NUMBER, GALYLEO_NUMBER])
}
for table in tables:
    server = ServeFromPandas(tables[table])
    framework.add_table_server(table, server.server)
    
