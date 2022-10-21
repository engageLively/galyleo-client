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


from tabnanny import check
from galyleo.galyleo_table_server import Filter, GalyleoDataServer, check_valid_spec
from galyleo.galyleo_constants import GALYLEO_STRING, GALYLEO_NUMBER, GALYLEO_BOOLEAN, GALYLEO_DATE, GALYLEO_DATETIME, GALYLEO_TIME_OF_DAY
from galyleo.galyleo_exceptions import InvalidDataException
import pytest
import pandas as pd

def test_check_filter():
    # type of argument test
    for test_var in [[], set(), ['a', 'b', 'c'], 'a', 1, None]:
        with pytest.raises(InvalidDataException, match=f'filter_spec must be a dictionary, not {type(test_var)}'):
            check_valid_spec(test_var)
    no_operator_spec = {'foo': 'bar'}
    with pytest.raises(InvalidDataException, match=f'There is no operator in {no_operator_spec}'):
        check_valid_spec(no_operator_spec)
    no_operator_spec = {'operator': 'bar'}    
    valid_operators = {'AND', 'OR', 'NOT', 'IN_LIST', 'IN_RANGE'}
    with pytest.raises(InvalidDataException, match=f'bar is not a valid operator.  Valid filter operators are {valid_operators}'):
        check_valid_spec(no_operator_spec)
    bad_fields = [
        ({'operator': 'AND'}, {'arguments'}),
        ({'operator': 'AND', 'foo': 'bar'}, {'arguments'}),
        ({'operator': 'AND', 'values': [1,2]}, { 'arguments'}),
        ({'operator': 'AND', 'argument': [1,2]}, {'arguments'}),
        ({'operator': 'OR'}, {'arguments'}),
        ({'operator': 'OR', 'foo': 'bar'}, {'arguments'}),
        ({'operator': 'OR', 'values': [1,2]}, {'arguments'}), 
        ({'operator': 'OR', 'argument': [1,2]}, {'arguments'}),
        ({'operator': 'NOT'},  {'argument'}),
        ({'operator': 'NOT', 'foo': 'bar'},  {'argument'}),
        ({'operator': 'NOT', 'values': [1,2]},  {'argument'}),
        ({'operator': 'NOT', 'arguments': [1,2]},  {'argument'}),
        ({'operator': 'IN_RANGE'}, {'column', 'max_val', 'min_val'}),
        ({'operator': 'IN_RANGE', 'max_val': 10, 'min_val': 5}, {'column'}),
        ({'operator': 'IN_RANGE', 'column': 'a'}, {'max_val, min_val'}), 
        ({'operator': 'IN_RANGE', 'column': 'a', 'max_val': 10}, {'min_val'}),
        ({'operator': 'IN_RANGE', 'column': 'a', 'min_val': 10}, {'max_val'}),
        ({'operator': 'IN_LIST'}, {'column', 'values'}),
        ({'operator': 'IN_LIST', 'column': 'bar'}, {'column'}),
        ({'operator': 'IN_LIST', 'values': [1,2]}, {'column'}),
    ]
    for spec in bad_fields:
         with pytest.raises(InvalidDataException, match='is missing required fields'):
             check_valid_spec(spec[0])
    # Do a bad list test for AND/OR
    with pytest.raises(InvalidDataException, match=f'The arguments field for AND must be a list, not {type({"a": "b"})}'):
        check_valid_spec({"operator": 'AND', "arguments": {"a": "b"}})
    
    
    good_spec = {"operator": "IN_RANGE", "min_val": 0, "max_val": 10, "column": "a"}
    bad_spec = {"operator": "IN_RANGE", "min_val": 0, "max_val": 10}
    # first, make sure this is OK with two good specs
    check_valid_spec({"operator": "AND", "arguments": [good_spec, good_spec]})
    with pytest.raises(InvalidDataException, match='is missing required fields'):
        check_valid_spec({"operator": "AND", "arguments": [good_spec, bad_spec]})
    check_valid_spec({"operator": "NOT", "argument": good_spec})
    with pytest.raises(InvalidDataException, match='is missing required fields'):
        check_valid_spec({"operator": "NOT", "argument": bad_spec})
    check_valid_spec(good_spec)
    bad_spec['column'] = 1
    # make sure ints are valid column names
    check_valid_spec(bad_spec)
    # Check bad column types
    for column_val in [1.0, None, {"a"}, {"a": "b"}]:
        bad_spec['column'] = column_val
        with pytest.raises(InvalidDataException, match=f'The column argument to IN_RANGE must be a string or an int, not {type(bad_spec["column"])}'):
            check_valid_spec(bad_spec)
    # Check bad lists
    good_list = [1, 2, 3, 4]
    bad_lists = [None, {}, {"a": "b"}, '1', 3]
    good_list_spec = {"operator": "IN_LIST", "column": "a", "values": good_list}
    check_valid_spec(good_list_spec)
    for bad_list in bad_lists:
        with pytest.raises(InvalidDataException, match = f'The Values argument to IN_LIST must be a list, not {type(bad_list)}'):
            check_valid_spec({"operator": "IN_LIST", "column": "a", "values": bad_list})
    # lists with bad types:
    bad_lists = [[None], [[1, 2], 1], [(1, 2), "a"], [{1,2}], [{"s": "p"}]]
    check_valid_spec({"operator": "IN_LIST", "column": "a", "values": []})
    for bad_list in bad_lists:
        with pytest.raises(InvalidDataException, match = 'Invalid Values'):
            check_valid_spec({"operator": "IN_LIST", "column": "a", "values": bad_list})
    good_ranges = [{"operator": "IN_RANGE", 'column': 'a', 'max_val': 20, 'min_val': 10}, {"operator": "IN_RANGE", 'column': 'b', 'max_val': 1.0, 'min_val': -0.3}]
    bad_ranges = [
        {"operator": "IN_RANGE", 'column': 'b', 'max_val': 10, 'min_val': '-3'},
        {"operator": "IN_RANGE", 'column': 'b', 'max_val': 10, 'min_val': None}
    ]
    for spec in good_ranges:
        check_valid_spec(spec)
    for bad_range in bad_ranges:
        bad_type = type(bad_range['min_val'])
        message = f'The type of min_val for IN_RANGE must be a number, not {bad_type}'
        with pytest.raises(InvalidDataException, match=message):
            check_valid_spec(bad_range)
    # Make sure recursion works well with good arguments:
    spec1 = {"operator": 'AND', 'arguments': [good_ranges[0], good_ranges[1], good_list_spec]}
    spec2 = {"operator": 'OR', 'arguments': [good_ranges[0], good_ranges[1], good_list_spec]}
    spec3 = {"operator": 'NOT', 'argument':  good_list_spec}
    spec4 = {"operator": 'AND', 'arguments': [spec1, good_list_spec]}
    spec5 = {"operator": 'OR', 'arguments': [spec1, spec4]}
    spec6 = {"operator": 'NOT', 'argument': spec5}
    good_specs = [spec1, spec2, spec3, spec4, spec5, spec6]
    for spec in good_specs: check_valid_spec(spec)
    bad_spec_1 = {'operator': 'NOT', 'argument': bad_ranges[0]}
    bad_spec_2 = {'operator': 'AND', 'arguments': [bad_ranges[1], good_ranges[0]]}
    bad_spec_3 = {'operator': 'OR', 'arguments': [bad_ranges[1], good_ranges[0]]}
    bad_spec_4 = {'operator': 'AND', 'arguments': [spec1, bad_spec_1]}
    bad_spec_5 = {'operator': 'OR', 'arguments': [spec2, bad_spec_4]}
    bad_spec_6 = {'operator': 'NOT', 'argument': bad_spec_4}
    bad_specs = [bad_spec_1, bad_spec_2, bad_spec_3, bad_spec_4, bad_spec_5, bad_spec_6]
    for spec in bad_specs:
        with pytest.raises(InvalidDataException):
            check_valid_spec(spec)


def test_in_list():
    filter_spec = {"operator": "IN_LIST", 'column': 'a', 'values': [1, 2, 3]}
    filter = Filter(filter_spec, ['a', 'b', 'c'])
    assert(filter.operator == 'IN_LIST')
    assert(filter.value_list == [1, 2, 3])
    assert(filter.column == 0)
    filter_spec = {"operator": "IN_LIST", 'column': 'b', 'values': ['a', 'b', 'c']}
    filter = Filter(filter_spec, ['a', 'b', 'c'])
    assert(filter.operator == 'IN_LIST')
    assert(filter.value_list == ['a', 'b', 'c'])
    assert(filter.column == 1)
    try:
        filter_spec = {"operator": "IN_LIST", 'column': 'a', 'values': [1, 2, 3]}
        filter = Filter(filter_spec, [])
    except InvalidDataException:
        return
    assert False, "Invalid Data Exception for missing column not raised"

def test_in_range():
    filter_spec = {"operator": "IN_RANGE", 'column': 'a', 'max_val': 20, 'min_val': 10}
    filter = Filter(filter_spec, ['a', 'b', 'c'])
    assert(filter.operator == 'IN_RANGE')
    assert(filter.max_val == 20)
    assert(filter.min_val == 10)
    assert(filter.column == 0)
    filter_spec = {"operator": "IN_RANGE", 'column': 'b', 'max_val': 10, 'min_val': -3}
    filter = Filter(filter_spec, ['a', 'b', 'c'])
    assert(filter.operator == 'IN_RANGE')
    assert(filter.max_val == 10)
    assert(filter.min_val == -3)
    assert(filter.column == 1)
    try:
        filter_spec = {"operator": "IN_LIST", 'column': 'b', 'max_val': 10, 'min_val': -3}
        filter = Filter(filter_spec, [])
    except InvalidDataException:
        return
    assert False, "Invalid Data Exception for missing column not raised"

def test_and():
    filter1 = {"operator": "IN_RANGE", 'column': 'a', 'max_val': 20, 'min_val': 10}
    filter2 = {"operator": "IN_LIST", 'column': 'b', 'values': [1, 2, 3]}
    filter_spec = {
        "operator": "AND",
        "arguments": [ filter1, filter2 ]
    }
    columns = ['a', 'b']
    filter = Filter(filter_spec, columns)
    assert(filter.operator == 'AND')
    assert(len(filter.arguments) == 2)
    f1 = Filter(filter1, columns)
    assert(f1.operator == 'IN_RANGE' and f1.column == 0 and f1.max_val == 20 and f1.min_val == 10)
    f2 = Filter(filter2, columns)
    assert(f2.operator == 'IN_LIST' and f2.column == 1 and f2.value_list == [1, 2, 3])
    # Test recursive missing column
    try:
        filter = Filter(filter_spec, ['a'])
    except InvalidDataException:
        return
    assert False, "Invalid Data Exception for missing column not raised"


def test_or():
    filter1 = {"operator": "IN_RANGE", 'column': 'a', 'max_val': 20, 'min_val': 10}
    filter2 = {"operator": "IN_LIST", 'column': 'b', 'values': [1, 2, 3]}
    filter_spec = {
        "operator": "OR",
        "arguments": [ filter1, filter2 ]
    }
    columns = ['a', 'b']
    filter = Filter(filter_spec, columns)
    assert(filter.operator == 'OR')
    assert(len(filter.arguments) == 2)
    f1 = filter.arguments[0]
    assert(f1.operator == 'IN_RANGE' and f1.column == 0 and f1.max_val == 20 and f1.min_val == 10)
    f2 = filter.arguments[1]
    assert(f2.operator == 'IN_LIST' and f2.column == 1 and f2.value_list == [1, 2, 3])
    try:
        filter = Filter(filter_spec, ['a'])
    except InvalidDataException:
        return
    assert False, "Invalid Data Exception for missing column not raised"

def test_not():
    filter1 = {"operator": "IN_RANGE", 'column': 'a', 'max_val': 20, 'min_val': 10}
    filter_spec = {
        "operator": "NOT",
        "argument": filter1
    }
    columns = ['a']
    filter = Filter(filter_spec, columns)
    assert(filter.operator == 'NOT')
    f1 = filter.argument
    assert(f1.operator == 'IN_RANGE' and f1.column == 0 and f1.max_val == 20 and f1.min_val == 10)
    try:
        filter = Filter(filter_spec, ['b'])
    except InvalidDataException:
        return
    assert False, "Invalid Data Exception for missing column not raised"

# rows for the tests of filters
rows = [
    ['a', 6, -1], ['b', 7, 3], ['c', 7, 2], ['a', 3, -1]
]

columns = ['a', 'b', 'c']

def test_filter_range():
    filter_spec = {"operator": "IN_RANGE", 'column': 'b', 'max_val': 6, 'min_val': 3}
    filter = Filter(filter_spec, columns)
    assert(filter._filter_index(rows) == {0, 3} )
    assert(filter.filter(rows) ==[['a', 6, -1],  ['a', 3, -1]] )
    filter_spec["max_val"] = 1
    filter_spec["min_val"] = 0
    filter = Filter(filter_spec, columns)
    assert(len(filter._filter_index(rows)) == 0 )
    assert(filter.filter(rows) ==[] )
    filter_spec["max_val"] = 7
    filter_spec["min_val"] = 6
    filter = Filter(filter_spec, columns)
    assert(filter._filter_index(rows) == {0, 1, 2} )
    assert(filter.filter(rows) ==[['a', 6, -1], ['b', 7, 3], ['c', 7, 2]] )

def test_filter_in_list():
    filter_spec = {"operator": "IN_LIST", 'column': 'a', 'values': ['a', 'b']}
    filter = Filter(filter_spec, columns)
    assert(filter._filter_index(rows) == {0, 1, 3} )
    assert(filter.filter(rows) ==[['a', 6, -1],  ['b', 7, 3], ['a', 3, -1]] )
    filter_spec["values"] = ['d']
    filter = Filter(filter_spec, columns)
    assert(len(filter._filter_index(rows)) == 0 )
    assert(filter.filter(rows) ==[] )
    filter_spec["values"] = ['a']
    filter = Filter(filter_spec, columns)
    assert(filter._filter_index(rows) == {0, 3} )
    assert(filter.filter(rows) ==[['a', 6, -1], ['a', 3, -1]] )
    filter_spec["values"] = []
    filter = Filter(filter_spec, columns)
    assert(len(filter._filter_index(rows)) == 0 )
    assert(filter.filter(rows) ==[] )

def is_partition(aSet, subset1, subset2):
    return (aSet == subset1 | subset2) and (len(subset1 & subset2) == 0)

def test_filter_not():
    inner_filter_spec = {"operator": "IN_LIST", 'column': 'a', 'values': ['a', 'b']}
    inner_filter = Filter(inner_filter_spec, columns)
    filter_spec = {
        "operator": "NOT",
        "argument": inner_filter_spec
    }
    filter = Filter(filter_spec, columns)
    universal = set(range(len(rows)))
    assert(is_partition(universal, filter._filter_index(rows), inner_filter._filter_index(rows)))

def test_filter_and():
    filter_spec1 = {"operator": "IN_LIST", 'column': 'a', 'values': ['a', 'b']}
    filter_spec2 = {"operator": "IN_RANGE", 'column': 'b', 'min_val': 6, 'max_val': 7}
    filter_spec = {
        "operator": "AND",
        "arguments": [filter_spec1, filter_spec2]
    }
    filter = Filter(filter_spec, columns)
    assert(filter._filter_index(rows) == {0, 1})
    filter.arguments = [filter.arguments[0]]
    assert(filter._filter_index(rows) == {0, 1,  3})
    filter.arguments = []
    assert(filter._filter_index(rows) == {0, 1, 2, 3})

def test_filter_or():
    filter_spec1 = {"operator": "IN_LIST", 'column': 'a', 'values': ['a', 'b']}
    filter_spec2 = {"operator": "IN_RANGE", 'column': 'b', 'min_val': 6, 'max_val': 7}
    filter_spec = {
        "operator": "OR",
        "arguments": [filter_spec1, filter_spec2]
    }
    filter = Filter(filter_spec, columns)
    assert(filter._filter_index(rows) == {0, 1, 2, 3})
    filter.arguments = [filter.arguments[0]]
    assert(filter._filter_index(rows) == {0, 1,  3})
    filter.arguments = []
    assert(len(filter._filter_index(rows)) == 0)

def presidential_vote_rows():
    frame  = pd.read_csv('tests/presidential_vote.csv')
    return frame.to_numpy().tolist()

def presidential_vote_column_values(column_name):
    frame  = pd.read_csv('tests/presidential_vote.csv')
    result = list(set(frame[column_name].tolist()))
    result.sort()
    return result

# test forming a Galyleo Data Server

def test_galyleo_server():
    presidential_names = ['Year', 'State', 'Name', 'Party', 'Votes', 'Percentage']
    presidential_types = [GALYLEO_NUMBER, GALYLEO_STRING, GALYLEO_STRING, GALYLEO_STRING, GALYLEO_NUMBER, GALYLEO_NUMBER]
    schema = [{"name": presidential_names[i], "type": presidential_types[i]} for i in range(len(presidential_names))]
    server = GalyleoDataServer(schema, presidential_vote_rows)
    assert(server._column_names() == presidential_names)
    assert(server.get_rows() == presidential_vote_rows())
    with pytest.raises(InvalidDataException, match='foo is not a column of this table'):
        server.all_values('foo')
    states = server.all_values('State')
    states1 = presidential_vote_column_values('State')
    assert(states == states1)
    with pytest.raises(InvalidDataException, match='foo is not a column of this table'):
        server.numeric_spec('foo')
    with pytest.raises(InvalidDataException, match=f'The type of Party must be {GALYLEO_NUMBER}, not {GALYLEO_STRING}'):
        server.numeric_spec('Party')
    assert(server.numeric_spec('Year') == {"max_val": 2020, "min_val": 1828, "increment": 4})
    # We've already tested Filter, so testing get_filtered_rows just makes sure that
    # calling the filter explicitly on the rows does the same thing as calling it through
    # GalyleoDataServer
    rows = presidential_vote_rows()
    filter_spec = {"operator": "IN_LIST", "column": 'State', "values": ["California", "Hawaii"]}
    filter = Filter(filter_spec, presidential_names)
    filtered_rows = filter.filter(rows)
    server_rows = server.get_filtered_rows(filter_spec)
    assert(filtered_rows == server_rows) 

