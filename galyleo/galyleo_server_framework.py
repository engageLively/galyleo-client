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

from os import lseek
from flask import Flask, request, abort
from galyleo.galyleo_exceptions import InvalidDataException
from galyleo.galyleo_table_server import GalyleoDataServer, check_valid_spec
from json import loads, dumps

class GalyleoServerFramework:
    '''
    A framework to easily and quickly implement a web server which serves tables according to 
    the Galyleo URL protocol.  This implements the URL methods get_filtered_rows, get_all_values,
    and get_numeric_spec.  It parses the arguments, checking for errors, takes the
    table argument, looks up the appropriate GalyleoDataServer to serve for that table, and
    then calls the method on that server to serve the request.  If no exception is thrown,
    returns a 200 with the result as a JSON structure, and if an exception is thrown, returns
    a 400 with an approrpriate error message.
    '''
    def __init__(self, app):
        app.add_url_rule('/get_filtered_rows', view_func = self.get_filtered_rows, methods=['POST'])
        app.add_url_rule('/get_numeric_spec', view_func = self.get_numeric_spec, methods=['GET'])
        app.add_url_rule('/get_all_values', view_func = self.get_all_values, methods=['GET'])
        app.add_url_rule('/hello', view_func = self._hello, methods = ['GET'])
        self.table_servers = {}
    
    def _hello(self):
        return 'hello, world'

    def add_table_server(self, table_name, galyleo_data_server):
        '''
        Register a GalyleoDataServer to server data for a specific table name.  
        '''
        self.table_servers[table_name] = galyleo_data_server

    def _get_table_server(self, table_name):
        try:
            return self.table_servers[table_name]
        except KeyError:
            abort(400, f'No handler defined for table {table_name}')

        
    def get_filtered_rows(self):
        parameters = request.get_json()
        if 'table_name' in parameters:
            server = self._get_table_server(parameters['table_name'])
            if 'filter_spec' in parameters:
                try:
                    filter_spec = parameters['filter_spec']
                    check_valid_spec(filter_spec)
                    return server.get_filtered_rows(filter_spec)
                except InvalidDataException as e:
                    abort(400, e)
            else:
                return dumps(server.get_rows())
        else:
            abort(400, 'table_name must be supplied to get_filtered_rows')

    def _check_required_parameters(self, handle, parameter_set):
        sent_parameters = set(request.args.keys())
        missing_parameters = parameter_set - sent_parameters
        if (len(missing_parameters) > 0):
            abort(400, f'Missing arguments to {handle}: {missing_parameters}')


    def get_numeric_spec(self):
        self._check_required_parameters('/get_numeric_spec', {'table_name', 'column_name'})
        server = self._get_table_server(request.args.get('table_name'))
        try:
            return dumps(server.numeric_spec(request.args.get('column_name')))
        except InvalidDataException as e:
            abort(400, e) 

    def get_all_values(self):
        self._check_required_parameters('/get_all_values', {'table_name', 'column_name'})
        server = self._get_table_server(request.args.get('table_name'))
        try:
            return dumps(server.all_values(request.args.get('column_name')))
        except InvalidDataException as e:
            abort(400, e) 

    
    
    
    