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


from galyleo.galyleo_table import GalyleoTable
from galyleo.galyleo_constants import GALYLEO_STRING, GALYLEO_NUMBER, GALYLEO_BOOLEAN, GALYLEO_DATE, GALYLEO_DATETIME, GALYLEO_TIME_OF_DAY
import pytest
from galyleo.galyleo_exceptions import InvalidDataException
import pandas as pd
import numpy
from json import dumps

#
# Test creation of a table, simple empty initialization
#

def test_create():
    table = GalyleoTable('test1')
    assert(table.name == 'test1')
    assert(table.schema == [])
    assert(table.data == [])

#
# Test equality.  Make sure that equal tables test equal, unequal ones
# test unequally, both with name matches and without
#
def test_equality():
    schema1 = [{"name": "a", "type": "string"}, {"name": "b", "type": "number"}]
    schema2 = [{"name": "a", "type": "string"}, {"name": "b", "type": "number"}]
    data1 = [["a", 1]]
    data2 = [["a", 1]]
    table1 = GalyleoTable('one')
    table2 = GalyleoTable('two')
    table1.load_from_dictionary({"columns": schema1, "rows": data1})
    table2.load_from_dictionary({"columns": schema2, "rows": data2})
    # test equality
    assert(table1.equal(table2))
    # test name mismatches
    assert(not table1.equal(table2, True))
    # test data mismatches
    data3 = [["b", 1]]
    table2.data = data3
    assert(not(table1.equal(table2)))
    # test schema mismatches
    table2.schema = [{"name": "a", "type": "string"}, {"name": "c", "type": "number"}]
    table2.data = data2
    assert(not(table1.equal(table2)))



# 
# Test _check_schema_match.  Make sure that good schemas pass
# and bad ones don't
#
def test_check_schema_match():
    # no assertions for schemas that should work
    table = GalyleoTable('test1')
    schema1 = [('a', GALYLEO_STRING), ('b', GALYLEO_NUMBER), ('c', GALYLEO_NUMBER)]
    schema2 =  [('a', GALYLEO_STRING), ('b', GALYLEO_NUMBER)]
    schema3 =  [('a', GALYLEO_STRING), ('b', GALYLEO_BOOLEAN)]
    
    data1 = [['a', 1, 2], ['b', 3, 4]]
    data2 = [['a', 1], ['b', 3]]
    data3 = [['a', True], ['b', False]]
    data4 = [['a', 'a'], ['b', 'b']]
    # check matches.  Throws an exception if fails, so no need for asserts
    table._check_schema_match(schema1, data1)
    table._check_schema_match(schema2, data2)
    table._check_schema_match(schema3, data3)
    # check mismatched lengths
    try:
        table._check_schema_match(schema1, data2)
        assert(False)
    except InvalidDataException:
        pass
    try:
        table._check_schema_match(schema2, data1)
        assert(False)
    except InvalidDataException:
        pass
    # check mismatched types
    try:
        table._check_schema_match(schema2, data4)
    except InvalidDataException:
         pass

#
# table to use to check loads
#
reference_table = GalyleoTable('reference_table')
reference_table.schema = [{"name": "name", "type": "string"}, {"name": "age", "type": "number"}]
reference_table.data = [['a', 1], ['b', 2]]

#
# test load_from_schema_and_data.  Since we've already tested _check_schema_match,
# all we need to do is test correct schemas and make sure that they
# load properly
#
def test_load_from_schema_and_data():
    schema = [('name', 'string'), ('age', 'number')]
    data = [['a', 1], ['b', 2]]
    table = GalyleoTable('test1')
    table.load_from_schema_and_data(schema, data)
    assert(table.equal(reference_table))

#
# test _match_type.  
#
def test_match_type():
    pass

#
# test load_from_dataframe
#

def test_load_from_dataframe():
    df_desc = {'name': ['a', 'b'], 'age': [1, 2]}
    dataframe = pd.DataFrame(df_desc)
    table = GalyleoTable('test')
    table.load_from_dataframe(dataframe)
    assert(table.equal(reference_table))


#
# test as_dictionary
#
def test_as_dictionary():
    schema = [{"name": "name", "type": "string"}, {"name": "age", "type": "number"}]
    data = [['a', 1], ['b', 2]]
    test_dict = reference_table.as_dictionary()
    assert(test_dict == {
        "name": reference_table.name,
        "table": {"columns": schema, "rows": data}
        })


#
# test load_from_dictionary
#
def test_load_from_dictionary():
    schema = [{"name": "name", "type": "string"}, {"name": "age", "type": "number"}]
    data = [['a', 1], ['b', 2]]
    table = GalyleoTable('test')
    test_dict = {"columns": schema, "rows": data}
    table.load_from_dictionary(test_dict)
    assert(table.equal(reference_table))

#
# test from_json
#
def test_from_json():
    schema = [{"name": "name", "type": "string"}, {"name": "age", "type": "number"}]
    data = [['a', 1], ['b', 2]]
    table = GalyleoTable('test')
    test_dict = {
        "name": reference_table.name,
        "table": {"columns": schema, "rows": data}
        }
    as_json = dumps(test_dict)
    table.from_json(as_json, False)
    assert(table.equal(reference_table))
    table.from_json(as_json, True)
    assert(table.equal(reference_table, True))

#
# test _check_fields
#
def test__check_fields():
    pass

#
# test to_json
#
def test_to_json():
    schema = [{"name": "name", "type": "string"}, {"name": "age", "type": "number"}]
    data = [['a', 1], ['b', 2]]
    test_dict = {
        "name": reference_table.name,
        "table": {"columns": schema, "rows": data}
        }
    as_json = dumps(test_dict)
    reference_json = reference_table.to_json()
    assert(as_json == reference_json)
