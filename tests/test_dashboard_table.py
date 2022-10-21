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


from galyleo.galyleo_table import GalyleoTable, RemoteGalyleoTable
from galyleo.galyleo_constants import GALYLEO_STRING, GALYLEO_NUMBER, GALYLEO_BOOLEAN, GALYLEO_DATE, GALYLEO_DATETIME, GALYLEO_TIME_OF_DAY
import pytest
from galyleo.galyleo_exceptions import InvalidDataException
import pandas as pd
import numpy
from json import dumps
import csv

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

#
# A utility that tests if two lists of lists are equal.   Only works
# if the entries in each row of both lists are hashable (strings, ints, tuples)
#
def lists_equal(list1, list2):
    tuples1 = [tuple(row) for row in list1]
    tuples2 = [tuple(row) for row in list2]
    set1 = set(tuples1)
    set2 = set(tuples2)
    return set1 == set2


#
# test aggregate_columns
#

def test_aggregate_by():
    schema = [{"name": "country", "type": GALYLEO_STRING}, {"name": "rating", "type": GALYLEO_NUMBER}]
    countries = ['USA', 'Canada', 'Australia']
    ratings = range(1, 4)
    data = [[country, rating] for country in countries for rating in ratings]
    data = data + [['USA', rating] for rating in ratings]
    test_table = GalyleoTable('test')
    test_table.load_from_dictionary({"columns": schema, "rows": data})
    with pytest.raises(InvalidDataException, match='No columns specified for aggregation'):
        test_table.aggregate_by(None, "count")
    with pytest.raises(InvalidDataException, match='No columns specified for aggregation'):
        test_table.aggregate_by([], "count")    
    with pytest.raises(InvalidDataException, match="Columns {'state'} are not present in the schema"):
        test_table.aggregate_by(['country', 'state'], "count")
    table2 = test_table.aggregate_by(["country"])
    expected_result = [['USA', 6], ['Canada', 3], ['Australia', 3]]
    assert(lists_equal(expected_result, table2.data))
    names = [entry["name"] for entry in table2.schema]
    assert(names == ["country", "count"])
    types = [entry["type"] for entry in table2.schema]
    assert(types == [GALYLEO_STRING, GALYLEO_NUMBER])
    assert(table2.name == "aggregate_c")
    table3 = test_table.aggregate_by(["country"], "mentions", "table_name")
    assert(table3.name == "table_name")
    names = [entry["name"] for entry in table3.schema]
    assert(names == ["country", "mentions"])
    table4 = test_table.aggregate_by(["country", "rating"])
    expected_result = [[country, i, 2 if country == "USA" else 1] for i in ratings for country in countries]
    assert(lists_equal(expected_result, table4.data))
    names = [entry["name"] for entry in table4.schema]
    assert(names == ["country", "rating", "count"])
    types = [entry["type"] for entry in table4.schema]
    assert(types == [GALYLEO_STRING, GALYLEO_NUMBER, GALYLEO_NUMBER])
    assert(table4.name == "aggregate_cr")

#
# A table to be used in testing filtering
#

def filter_test_table():
    table = GalyleoTable('filter_test')
    schema = [{"name": "country", "type": GALYLEO_STRING}, {"name": "rating", "type": GALYLEO_NUMBER}, {"name": "population", "type": GALYLEO_NUMBER}]
    data =[ ['USA', 3, 340000], ['Canada', 5, 39000], ['Australia', 4, 24000], ['France', 3, 66000]]
    table.load_from_dictionary({"columns": schema, "rows": data})
    return table

#
# test filter_by_function
#
def test_filter_by_function():
    table = filter_test_table()
    with pytest.raises(InvalidDataException, match='new_table_name cannot be empty'):
        table.filter_by_function('foo', lambda x: x > 3, '')
    with pytest.raises(InvalidDataException, match='column_name cannot be empty'):
        table.filter_by_function('', lambda x: x > 3, 'foo')
    with pytest.raises(InvalidDataException, match='Column foo not found'):
        table.filter_by_function('foo', lambda x: x > 3, 'foo')
    with pytest.raises(InvalidDataException, match=f'Type {GALYLEO_STRING} not found in *'):
        table.filter_by_function('country', lambda x: x > 3, 'foo', {GALYLEO_NUMBER})
    table2 = table.filter_by_function('country', lambda x: x == 'USA', 'foo')
    assert(table2.schema == table.schema[1:])
    assert(table2.data == [[3, 340000]])
    assert(table2.name == 'foo')
    table3 = table.filter_by_function('rating', lambda x: x > 3, 'bar', {GALYLEO_NUMBER})
    assert(table3.schema == [table.schema[0], table.schema[2]])
    assert(table3.data == [['Canada', 39000], ['Australia', 24000]])
    assert(table3.name == 'bar')
    table4 = table.filter_equal('rating', 3, 'bar', {GALYLEO_NUMBER})
    assert(table4.schema == [table.schema[0], table.schema[2]])
    assert(table4.data == [['USA', 340000], ['France', 66000]])
    assert(table4.name == 'bar')
    with pytest.raises(InvalidDataException, match=' should be a tuple of length 2'):
        table.filter_range('rating', None, 'foo', {GALYLEO_NUMBER})
    with pytest.raises(InvalidDataException, match='3 should be a tuple of length 2'):
        table.filter_range('rating', 3, 'foo', {GALYLEO_NUMBER})
    with pytest.raises(InvalidDataException):
        table.filter_range('rating', (1, 2, 3), 'foo', {GALYLEO_NUMBER})
    table5 = table.filter_range('population', (39000, 70000), 'bar', {GALYLEO_NUMBER})
    assert(table5.schema == table.schema[:2])
    assert(table5.data == [['Canada', 5], ['France', 3]])
    assert(table5.name == 'bar')

def test_pivot_on_column():

    def make_data_row(row):
        cleaned = [entry.strip() for entry in row]
        return [int(entry) for entry in cleaned[0:3]] + cleaned[3:7] + [float(cleaned[7])]
    with open('tests/pivot_test_file.csv', 'r') as ufo_file:
        ufo_reader = csv.reader(ufo_file)
        header = next(ufo_reader)
        data = [make_data_row(row) for row in ufo_reader]
    names = [entry.strip() for entry in header]
    types = [GALYLEO_NUMBER for i in range(3)] + [GALYLEO_STRING for i in range(4)] + [GALYLEO_NUMBER]
    schema = [{"name": names[i], "type": types[i]} for i in range(len(types))]
    t1 = GalyleoTable("t1")
    t1.load_from_dictionary({"columns": schema, "rows": data})
    t2 = t1.aggregate_by(["country", "year", "month", "timeofday"])
    with pytest.raises(InvalidDataException, match = 'new_table_name cannot be empty'):
        t2.pivot_on_column("timeofday", "count", "")
    with pytest.raises(InvalidDataException, match = 'pivot_column_name cannot be empty'):
        t2.pivot_on_column("", "count", "tod_pivot")
    with pytest.raises(InvalidDataException, match = 'value_column_name cannot be empty'):
        t2.pivot_on_column("timeofday", "", "tod_pivot")
    with pytest.raises(InvalidDataException, match = 'Pivot and value columns cannot be identical: both are count'):
        t2.pivot_on_column("count", "count", "tod_pivot")
    t3 = t2.pivot_on_column("timeofday", "count", "tod_pivot")
    column_names = set([entry["name"] for entry in t3.schema])
    assert(column_names == {"country", "year", "month", "morning", "afternoon",  "night"})
    column_types = [entry["type"] for entry in t3.schema]
    assert(column_types == [GALYLEO_NUMBER for i in range(2)] + [GALYLEO_STRING] + [GALYLEO_NUMBER for i in range(3)])
    t5 = t1.aggregate_by(["country", "type"])
    t6 = t5.pivot_on_column("type", "count", "type_pivot", {"disk", "triangle", "cylinder", "sphere"}, True)
    column_names = set([entry["name"] for entry in t6.schema])
    assert(column_names == {"country", "disk", "triangle", "cylinder", "sphere", "Other"})
    column_types = [entry["type"] for entry in t6.schema]
    assert(column_types == [GALYLEO_STRING] + [GALYLEO_NUMBER for i in range(5)])
    t7 = t1.aggregate_by(["type"])
    t8 = t7.pivot_on_column("type", "count", "pivot_with_other", {"disk"}, True)
    t9 = t7.pivot_on_column("type", "count", "pivot_without_other", {"disk"}, False)
    assert(len(t8.data) == 1 and len(t8.data[0]) == 2)
    assert(len(t9.data) == 1 and len(t9.data[0]) == 1)
    assert(t8.data[0][0] == t9.data[0][0])
    assert(t8.data[0][0] + t8.data[0][1] == len(t1.data))

# Test Remote GalyleoTable
def test_create_remote():
    schema = [{"name": "country", "type": GALYLEO_STRING}, {"name": "rating", "type": GALYLEO_NUMBER}, {"name": "population", "type": GALYLEO_NUMBER}]
    base_url = 'https://www.yahoo.com/foo/index.html'
    table = RemoteGalyleoTable('test1', schema, base_url)
    assert(table.name == 'test1')
    assert(table.schema == schema)
    assert(table.base_url == base_url)
    assert(table.header_variables == [])
    table = RemoteGalyleoTable('test1', schema, base_url, None)
    assert(table.name == 'test1')
    assert(table.schema == schema)
    assert(table.base_url == base_url)
    assert(table.header_variables == [])
    table = RemoteGalyleoTable('test1', schema, base_url, ['a', 'b'])
    assert(table.name == 'test1')
    assert(table.schema == schema)
    assert(table.base_url == base_url)
    assert(table.header_variables == ['a', 'b'])

#
# Test as_dictionary for remote tables
#

def test_as_dictionary_remote():
    schema = [{"name": "country", "type": GALYLEO_STRING}, {"name": "rating", "type": GALYLEO_NUMBER}, {"name": "population", "type": GALYLEO_NUMBER}]
    base_url = 'https://www.yahoo.com/foo/index.html'
    table = RemoteGalyleoTable('test1', schema, base_url)
    assert(table.as_dictionary() == {
        "name": "test1", "columns": schema, "table": {"base_url": base_url, "header_variables": []}
    })
    table = RemoteGalyleoTable('test1', schema, base_url, None)
    assert(table.as_dictionary() == {
        "name": "test1", "columns": schema, "table": {"base_url": base_url, "header_variables": []}
    })
    table = RemoteGalyleoTable('test1', schema, base_url, ['a', 'b'])
    assert(table.as_dictionary() == {
        "name": "test1", "columns": schema, "table": {"base_url": base_url, "header_variables": ['a', 'b']}
    })


    
