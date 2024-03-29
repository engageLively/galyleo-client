'''
A GalyleoDataServer class and associated utilities.  The GalyleoDataServer class is initialized
with the table's schema,  single function,get_rows(), which returns the rows of the table.  To
use a  GalyleoDataServer instance, instantiate it with the schema and a get_rows() function.
The GalyleoDataServer instance can then be passed to a GalyleoServer with a call to
galyleo_server_framework.add_table_server, and the server will then be able to serve
the tables automatically using the instantiated GalyleoDataServer.
'''

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

from functools import reduce
from math import nan
from galyleo.galyleo_constants import GALYLEO_NUMBER, GALYLEO_STRING, GALYLEO_BOOLEAN
from galyleo.galyleo_exceptions import InvalidDataException

def check_valid_spec(filter_spec):
    '''
    Class method which checks to make sure that a filter spec is valid.
    Does not return, but throws an InvalidDataException with an error message
    if the filter spec is invalid

    Arguments:
        filter_spec: spec to test for validity
    '''

    # Check to make sure filter_spec is a dictionary, and not something else
    if not isinstance(filter_spec, dict):
        raise InvalidDataException(f'filter_spec must be a dictionary, not {type(filter_spec)}')
    #
    # Step 1: check to make sure there is an operator field, and that it's an operator we recognize
    if 'operator' in filter_spec:
        operator = filter_spec['operator']
        valid_operators = {'ALL', 'ANY', 'NONE', 'IN_LIST', 'IN_RANGE'}
        if not operator in valid_operators:
            msg = f'{operator} is not a valid operator.  Valid operators are {valid_operators}'
            raise InvalidDataException(msg)
    else:
        raise InvalidDataException(f'There is no operator in {filter_spec}')
    # Check to make sure that the fields are right for the operator that was given
    # We don't throw an error for extra fields, just for missing fields. Since we're
    # going to use keys() to get the fields in the spec, and this will include the
    # operator, 'operator' is one of the fields
    fields = {
        'ALL': {'arguments', 'operator'},
        'ANY': {'arguments', 'operator'},
        'NONE': {'arguments', 'operator'},
        'IN_LIST': {'column', 'values', 'operator'},
        'IN_RANGE': {'column', 'max_val', 'min_val', 'operator'}
    }
    fields_in_spec = set(filter_spec.keys())
    missing_fields = fields[operator] - fields_in_spec
    if len(missing_fields) > 0:
        raise InvalidDataException(f'{filter_spec} is missing required fields {missing_fields}')
    # For ALL and ANY, recursively check the arguments list and return
    if (operator in {'ALL', 'ANY', 'NONE'}):
        if not isinstance(filter_spec['arguments'], list):
            bad_type = type(filter_spec["arguments"])
            msg = f'The arguments field for {operator} must be a list, not {bad_type}'
            raise InvalidDataException(msg)
        for arg in filter_spec['arguments']:
            check_valid_spec(arg)
        return
    # if we get here, it's IN_LIST or IN_RANGE.  For both, check that the column is a string
    if (not type(filter_spec['column']) in {str, int}):
        bad_type = type(filter_spec["column"])
        msg = f'The column argument to {operator} must be a string or an int, not {bad_type}'
        raise InvalidDataException(msg)
    # For IN_LIST, check that the values argument is a list and each item is a string or number
    if operator == 'IN_LIST':
        values_type = type(filter_spec['values'])
        if values_type == list:
            value_list = filter_spec['values']
            invalid = [value for value in value_list if not type(value) in {str, float, int}]
            if len(invalid) > 0:
                raise InvalidDataException(f'Invalid Values {invalid} for IN_LIST')
        else:
            msg = f'The Values argument to IN_LIST must be a list, not {values_type}'
            raise InvalidDataException(msg)
    else:
        # For IN_RANGE, make sure max_val and min_val are numbers
        for field in ['max_val', 'min_val']:
            if (not type(filter_spec[field]) in {int, float} ):
                bad_type = type(filter_spec[field])
                msg = f'The type of {field} for IN_RANGE must be a number, not {bad_type}'
                raise InvalidDataException(msg)


class Filter:
    '''
    A Class which implements a Filter used by VirtualGalyleoTable to filter rows.
    The arguments to the contstructor are a filterSpec, which is a boolean tree
    of filters and the columns which the filter is implemented over.
    Note that there is no error-checking here: it is assumed that the dashboard
    widgets will only select columns which appear in the tables, of the right type,
    and so on.
    This is designed to be instantiated from VirtualGalyleoTable.get_filtered_rows()
    and in no other place -- error checking, if any, should be done there.

    Arguments:
        filter_spec: a Specification of the filter as a dictionary.
        columns: the names of the columns (names alone, not types)
    '''
    def __init__(self, filter_spec, columns):
        self.operator = filter_spec["operator"]
        if (self.operator == 'ALL' or self.operator == 'ANY' or self.operator == 'NONE'):
            self.arguments = [Filter(argument, columns) for argument in filter_spec["arguments"]]
        elif self.operator == 'IN_LIST':
            try:
                self.column = columns.index(filter_spec["column"])
            except ValueError as original_error:
                raise InvalidDataException(f'{filter_spec["column"]} is not a valid column')  from original_error
            self.value_list = filter_spec['values']
        else: # operator is IN_RANGE
            try:
                self.column = columns.index(filter_spec["column"])
            except ValueError as original_error:
                raise InvalidDataException(f'{filter_spec["column"]} is not a valid column') from  original_error
            self.max_val = filter_spec['max_val']
            self.min_val = filter_spec['min_val']

    def filter(self, rows):
        '''
        Filter the rows according to the specification given to the constructor.
        Returns the rows for which the filter returns True.

        Arguments:
            rows: list of list of values, in the same order as the columns
        Returns:
            subset of the rows, which pass the filter
        '''
        # Just an overlay on filter_index, which returns the INDICES of the rows
        # which pass the filter.  This is the top-level call, filter_index is recursive
        indices = self.filter_index(rows)
        return [rows[i] for i in range(len(rows)) if i in indices]

    def filter_index(self, rows):
        '''
        Not designed for external call.
        Filter the rows according to the specification given to the constructor.
        Returns the INDICES of the  rows for which the filter returns True.
        Arguments:

            rows: list of list of values, in the same order as the columns

        Returns:
            INDICES of the rows which pass the filter, AS A SET
 
        '''
        all_indices = range(len(rows))
        if self.operator == 'ALL':
            argument_indices = [argument.filter_index(rows) for argument in self.arguments]
            return reduce(lambda x, y: x & y, argument_indices, set(all_indices))
        elif self.operator == 'ANY':
            argument_indices = [argument.filter_index(rows) for argument in self.arguments]
            return reduce(lambda x, y: x | y, argument_indices, set())
        elif self.operator == 'NONE':
            argument_indices = [argument.filter_index(rows) for argument in self.arguments]
            return reduce(lambda x, y: x - y, argument_indices, set(all_indices))
        elif self.operator == 'IN_LIST':
            values = [row[self.column] for row in rows]
            return set([i for i in all_indices if values[i] in self.value_list])
        else:
            values = [row[self.column] for row in rows]
            return set([i for i in all_indices if values[i] <= self.max_val and values[i] >= self.min_val])

def _convert_to_type(galyleo_type, value):
    '''
    Convert value to galyleo_type, so that comparisons can be done.  Currently only works for string, number, and boolean.
    We will add date and time later.
    Returns a default value if value can't be converted
    Note that it's the responsibility of the object which provides the rows to always provide the correct types,
    so this really should always just return value
    Arguments:
        galyleo_type: type to convert to
        value: value to be converted
    Returns:
        value cast to the correct type
    '''
    if galyleo_type == GALYLEO_STRING:
        if isinstance(value, str):
            return value
        try:
            return str(value)
        except ValueError:
            return '' # return a default value
    if galyleo_type == GALYLEO_NUMBER:
        if isinstance(value, int) or isinstance(value, float):
            return value
        try:
            return float(value)
        except ValueError:
            return nan
    if galyleo_type == GALYLEO_BOOLEAN:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value == 'True'
        return False
    return value

def _convert_list_to_type(galyleo_type, value_list):
    '''
    Convert value_list to galyleo_type, so that comparisons can be done.  Currently only works for lists of string, number, and boolean.
    We will add date and time later.
    Returns a default value if value can't be converted
    Note that it's the responsibility of the object which provides the rows to always provide the correct types,
    so this really should always just return a new copy of value_list
    Arguments:
        galyleo_type: type to convert to
        value_list: list of values to be converted
    Returns:
        value_list with each element cast to the correct type
    '''
    return [_convert_to_type(galyleo_type, elem) for elem in value_list]

DEFAULT_HEADER_VARIABLES = {"required": [], "optional": []}
'''
The Default for header variables for a table is both required and optional lists are empty.
'''
class GalyleoDataServer:
    '''
    A Galyleo Data Server: This is instantiated with a function get_rows() which  delivers the
    rows, rather than having them explicitly in the Table.  Note that get_rows() *must* return
    the appropriate number of columns of the appropriate types.

    Arguments:
        schema: a list of records of the form {"name": <column_name, "type": <column_type>}.
           The column_type must be a type from galyleo_constants.GALYLEO_TYPES.
        get_rows: a function which returns a list of list of values.  Each component list
            must have the same length as schema, and the jth element must be of the
            type specified in the jth element of schema
        header_variables: a dictionary of two fields, required and optional,
            both of which are lists of strings (variable names).  Either
            or both can be empty.  The variables (and their values) are
            passed with each table request
    '''
    def __init__(self, schema, get_rows, header_variables=None):
        self.schema = schema
        self.get_rows = get_rows
        self.header_variables = DEFAULT_HEADER_VARIABLES if header_variables is None else header_variables

    # This is used to get the names of a column from the schema

    def column_names(self):
        '''
        Return the names of the columns
        '''
        return [column["name"] for column in self.schema]

    def column_types(self):
        '''
        Return the types of the columns
        '''
        return [column["type"] for column in self.schema]

    def get_column_type(self, column_name):
        '''
        Returns the type of column column_name, or None if this table doesn't have a column with
        name  column_name.

        Arguments:
            column_name: name of the column to get the type for
        '''
        matches = [column["type"] for column in self.schema if column["name"] == column_name]
        if len(matches) == 0:
            return None
        else:
            return matches[0]
       

    def all_values(self, column_name:str):
        '''
        get all the values from column_name
        Arguments:

            column_name: name of the column to get the values for

        Returns:
            List of the values

        '''
        try:
            index = self.column_names().index(column_name)
        except ValueError as original_error:
            raise InvalidDataException(f'{column_name} is not a column of this table') from original_error
        galyleo_type = self.schema[index]["type"]
        rows = self.get_rows()
        result =  _convert_list_to_type(galyleo_type, list(set([row[index] for row in rows])))
        result.sort()
        return result

    def numeric_spec(self, column_name:str):
        '''
        get the dictionary {min_val, max_val, increment} for column_name
        Arguments:

            column_name: name of the column to get the numeric spec for

        Returns:
            the minimum, maximum, and increment of the column

        '''
        entry = [column for column in self.schema if column["name"] == column_name]
        if len(entry) == 0:
            raise InvalidDataException(f'{column_name} is not a column of this table')
        if entry[0]["type"] != GALYLEO_NUMBER:
            msg = f'The type of {column_name} must be {GALYLEO_NUMBER}, not {entry[0]["type"]}'
            raise InvalidDataException(msg)
        try:
            values = self.all_values(column_name)
            shift = values[1:]
            difference = [shift[i] - values[i] for i in range(len(shift))]
            increments = [diff for diff in difference if diff > 0]
            return {"max_val": values[-1], "min_val": values[0], "increment": min(increments)}
        except ValueError as original_error:
            raise InvalidDataException(f'Bad data in column {column_name}') from original_error

    def get_filtered_rows(self, filter_spec):
        '''
        Filter the rows according to the specification given by filter_spec.
        Returns the rows for which the resulting filter returns True.

        Arguments:
            filter_spec: Specification of the filter, as a dictionary
        Returns:
            The subset of self.get_rows() which pass the filter
        '''
        made_filter = Filter(filter_spec, self.column_names())
        return made_filter.filter(self.get_rows())
