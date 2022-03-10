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


from galyleo.galyleo_table_server import Filter, GalyleoDataServer
from galyleo.galyleo_constants import GALYLEO_STRING, GALYLEO_NUMBER, GALYLEO_BOOLEAN, GALYLEO_DATE, GALYLEO_DATETIME, GALYLEO_TIME_OF_DAY
from galyleo.galyleo_exceptions import InvalidDataException
import pytest

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
