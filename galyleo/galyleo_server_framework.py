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

import json
from os import lseek
from flask import Blueprint, request, abort, jsonify
from galyleo.galyleo_exceptions import InvalidDataException
from galyleo.galyleo_table_server import GalyleoDataServer, check_valid_spec
from json import loads, dumps
'''
A framework to easily and quickly implement a web server which serves tables according to 
the Galyleo URL protocol.  This implements the URL methods get_filtered_rows, get_all_values,
and get_numeric_spec.  It parses the arguments, checking for errors, takes the
table argument, looks up the appropriate GalyleoDataServer to serve for that table, and
then calls the method on that server to serve the request.  If no exception is thrown,
returns a 200 with the result as a JSON structure, and if an exception is thrown, returns
a 400 with an approrpriate error message.
All of the methods here except for add_table_server are simply route targets: none are designed for calls
from any method other than flask
'''

galyleo_server_blueprint = Blueprint('galyleo_server', __name__)

table_servers = {}

def _get_table_key(table_name, dashboard_name = None):
    '''
    The table key for a server (the dictionary key that finds the server) is either the 
    pair (dashboard_name, table_name) if dashboard_name is not None, or table_name if dashboard_name is None.
    This method just computes and returns that.  Internal use only
    Parameters:
        table_name: name of the table
        dashboard_name: name of the dashboard
    Returns:
        (dashboard_name, table_name) if dashboard_name is not None, table_name if table_name otherwise
    Raises:
        InvalidDataException if table_name is None
    '''
    if table_name == None:
        raise  InvalidDataException('table_name must be supplied')
    return table_name if dashboard_name == None else (dashboard_name, table_name)

def add_table_server(table_name, galyleo_data_server, dashboard_name = None):
    '''
    Register a GalyleoDataServer to serve data for a specific table name, and, optionally, dashboard_name
    if it is supplied.   Raises an InvalidDataException if table_name is None or galyleo_data_server is None
    or is not an instance of GalyleoDataServer
    Parameters:
        table_name: name to register the server for
        galyleo_data_server: an instange of GalyleoDataServer which services the requests
        dashboard_name: name of the dashboard (optional, None if not supplied)
    '''
    try:
        assert galyleo_data_server != None, "galyleo_data_server cannot be None"
        assert isinstance(galyleo_data_server, GalyleoDataServer), f'galyleo_data_server must be an instance of GalyleoDataServer, not {type(galyleo_data_server)}'
    except AssertionError as e:
        raise InvalidDataException(e)
    table_servers[_get_table_key(table_name, dashboard_name)] = galyleo_data_server

def _get_table_server(request_api):
    '''
    Internal use.  Get the server for a specific table_name and return it.
    Aborts the request with a 400 if the table isn't found.
    Parameters:
        request_api: api  of the request
    '''
    table_name = request.headers.get('Table-Name')
    dashboard_name = request.headers.get('Dashboard_Name')
    try:
        table_signature = _get_table_key(table_name, dashboard_name)
    except InvalidDataException:
        abort(400, f'No table name specified for {request_api}')
    try:
        return table_servers[table_signature]
    except KeyError:
        abort(400, f'No handler defined for table {table_signature} for request {request_api}')


def _check_required_parameters(handle, parameter_set):
    '''
    Check to make sure the required parameters are in the parameter set
    required for a request, aborting if they aren't. This can only be used
    with get requests, since it pulls this from the args multidict.
    This is designed for internal use only
    Parameters:
        handle: the URL handle, for error reporting
        parameter_set: the set of parameters required
    '''
    sent_parameters = set(request.args.keys())
    missing_parameters = parameter_set - sent_parameters
    if (len(missing_parameters) > 0):
        abort(400, f'Missing arguments to {handle}: {missing_parameters}')


    
@galyleo_server_blueprint.route('/hello')
def hello():
    '''
    Just a simple get target to make sure that the framework is working
    '''
    return "hello"

@galyleo_server_blueprint.route('/echo_headers', methods = ['POST', 'GET'])
def echo_headers():
    result = {}
    for key in request.headers.keys():
        result[key] = request.headers[key]
    return dumps(result)

@galyleo_server_blueprint.route('/echo_post', methods=['POST'])
def echo_post():
    '''
    Echo the request
    '''
    return jsonify(request.json)

@galyleo_server_blueprint.route('/get_filtered_rows', methods=['POST'])    
def get_filtered_rows():
    '''
    Get the filtered rows from a request.  In the initializer, this
    was registered for the /get_filtered_rows route.  Parses the
    incoming request, which will be in a form, finds the table from
    the table_name parameter and the filter_spec from the filter_spec
    parameter, and calls server.get_filtered_rows() to return the
    rows which match the filter.  If there is no filter_spec, returns
    all rows using server.get_rows().  Aborts with a 400 if there is no
    table_name, or if check_valid_spec or get_filtered_rows throws an
    InvalidDataException.
    Arguments:
        None
    Returns:
        The filtered rows as a JSONified list of lists
    '''
    parameters = request.json
    server = _get_table_server('get_filtered_rows')
    if 'filter_spec' in parameters:
        try:
            filter_spec = parameters['filter_spec']
            check_valid_spec(filter_spec)
            return dumps(server.get_filtered_rows(filter_spec))
        except InvalidDataException as e:
            abort(400, e)
    else:
        return dumps(server.get_rows())
    

@galyleo_server_blueprint.route('/get_numeric_spec')
def get_numeric_spec():
    '''
    Target for the /get_numeric_spec route.  Makes sure that table_name and column_name are specified
    in the call, and that table_name is registered, then returns the numeric spec {"min_val", "max_val", "increment"}
    as a JSONified dictionary.  Uses server.get_numeric_spec(column_name) to create the numeric spec.  Aborts
    with a 400 for missing arguments, bad table name, or if there is no column_name in the arguments.
    Parameters: 
            None
    '''
    server = _get_table_server('/get_numeric_spec')
    column_name = request.args.get('column_name')
    if (column_name != None):
        return dumps(server.numeric_spec(column_name))
    else:
        abort(400, '/get_numeric_spec requires a parameter "column_name"') 

@galyleo_server_blueprint.route('/get_all_values')
def get_all_values():
    '''
    Target for the /get_all_values route.  Makes sure that table_name and column_name are specified
    in the call, and that table_name is registered, then returns the distinct values a
    as a JSONified list.  Uses server.get_all_values(column_name) to get the values.  Aborts
    with a 400 for missing arguments, bad table name, or if there is no column_name in the arguments.
    Parameters: 
            None
    '''
    
    server = _get_table_server('/get_all_values')
    column_name = request.args.get('column_name')
    if column_name != None:
        return dumps(server.all_values(column_name))
    else:
        abort(400, '/get_numeric_spec requires a parameter "column_name"') 
    

@galyleo_server_blueprint.route('/get_tables')    
def get_tables():
    '''
    Target for the /get_tables route.  Dumps a JSONIfied dictionary of the form:
    {table_name: <table_schema>}, where <table_schema> is a dictionary
    {"name": name, "type": type}
    Parameters:
            None
    '''
    result = {}
    for key in table_servers.keys():
        result[key] = table_servers[key].schema
    return dumps(result)
