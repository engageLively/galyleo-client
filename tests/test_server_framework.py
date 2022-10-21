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


from urllib import response
from json import loads, dumps
import pytest
import pandas as pd
from flask import Flask, jsonify
from galyleo.galyleo_constants import GALYLEO_NUMBER, GALYLEO_STRING
from galyleo.galyleo_server_framework import  galyleo_server_blueprint, add_table_server
from galyleo.galyleo_table_server import GalyleoDataServer



def presidential_vote_rows():
    '''
    Get the rows of the presidential table
    '''
    frame  = pd.read_csv('tests/presidential_vote.csv')
    return frame.to_numpy().tolist()

def presidential_vote_column_values(column_name):
    '''
    Get the columns of the presidential table
    '''
    frame  = pd.read_csv('tests/presidential_vote.csv')
    result = list(set(frame[column_name].tolist()))
    result.sort()
    return result

'''
The names of the columns of the presidential table
'''
presidential_names = ['Year', 'State', 'Name', 'Party', 'Votes', 'Percentage']
'''
The types of the columns of the presidential table
'''
presidential_types = [GALYLEO_NUMBER, GALYLEO_STRING, GALYLEO_STRING, GALYLEO_STRING, GALYLEO_NUMBER, GALYLEO_NUMBER]
'''
The schema of the presidential table
'''
schema = [{"name": presidential_names[i], "type": presidential_types[i]} for i in range(len(presidential_names))]
'''
Build the server for the presidential table
'''
server = GalyleoDataServer(schema, presidential_vote_rows)
'''
table name, for convenience
'''
TABLE_NAME = 'presidential_table'
def make_get_prefix(route):
    '''
    A convenience function to make the prefix for a GET call
    '''
    return f'/{route}?table_name={TABLE_NAME}'

def test_server_framework():
    '''
    Test the server framework using the Blueprints strategy.
    The functionality has already been tested, so this is just
    to make sure the responses come back correctly
    '''
    app = Flask(__name__)
    app.register_blueprint(galyleo_server_blueprint, url_prefix='/')
    add_table_server(TABLE_NAME, server)
    client = app.test_client()
    galyleo_response = client.get('/hello')
    assert galyleo_response.status == '200 OK'
    # headers= Headers({"table_name": TABLE_NAME})
    headers = {"Table-Name": TABLE_NAME}
    
    # galyleo_response = client.get(f'{make_get_prefix("get_all_values")}&column_name=Year', )
    galyleo_response = client.get('/get_all_values?column_name=Year', headers = headers)
    assert galyleo_response.status == '200 OK'
    result = loads(galyleo_response.get_data(as_text = True))
    expected = [year for year in range(1828, 2021, 4)]
    assert(result == expected)
    # galyleo_response = client.get(f'{make_get_prefix("get_numeric_spec")}&column_name=Year')
    galyleo_response = client.get('/get_numeric_spec?column_name=Year', headers = headers)
    assert galyleo_response.status == '200 OK'
    expected = {"max_val": 2020, "min_val": 1828, "increment": 4}
    result = loads(galyleo_response.get_data(as_text = True))
    assert(result == expected)
    spec = {'operator': 'IN_RANGE', 'max_val': 1980, 'min_val': 1960, 'column': 'Year'}
    # data=dumps({'table_name': TABLE_NAME, 'filter_spec': spec})
    # data=dumps({'filter_spec': spec})
    # galyleo_response = client.get('/get_filtered_rows', data = data, content_type='application/json', headers = headers)
    headers ['Filter-Spec'] = dumps(spec)
    galyleo_response = client.get('/get_filtered_rows', headers = headers)
    assert galyleo_response.status == '200 OK'
    expected = [row for row in presidential_vote_rows() if row[0] >= 1960 and row[0] <= 1980]
    response = galyleo_response.get_data(as_text = True)
    result = loads(response)
    assert len(expected) == len(result)
    
    
    
    
    