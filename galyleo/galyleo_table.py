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

import gviz_api
import pandas as pd
from galyleo.galyleo_constants import GALYLEO_SCHEMA_TYPES
import numpy
from galyleo.galyleo_constants import GALYLEO_STRING, GALYLEO_NUMBER, GALYLEO_BOOLEAN, GALYLEO_DATE, GALYLEO_DATETIME, GALYLEO_TIME_OF_DAY
from json import loads, dumps, JSONDecodeError
from galyleo.galyleo_exceptions import InvalidDataException

#
# Initialize with  the table name
#
class GalyleoTable:
    '''
    A Galyleo Dashboard Table.  Used to create a Galyleo Dashboard Table from any of a number of sources, and then generate an object that is suitable
    for storage (as a JSON file).  A GalyleoTable is very similar to  a Google Visualization data table, and can be
    converted to a Google Visualization Data Table on either the Python or the JavaScript side.
    Convenience routines provided here to import data from pandas, and json format.
    '''
    def __init__(self, name:str):
        """
        The DashboardTable Class. Sets the schema and data to be empty, and the name to be name

        Args:
           name (str): The nameo of the table
        """
        self.name = name
        self.schema = []
        self.data = []

    def equal(self, table, names_must_match = False):
        """ 
        Test to see if this table is equal to another table, passed as
        an argument.  Two tables are equal if their schemas are the same
        length and column names and types match, and if the data is the same,
        and in the same order.  If names_must_match == True (default is False),
        then the names must also match

        Args:
           table (GalyleoTable): table to be checked for equality
           names_must_match (bool): (default False) if True, table names must also match
        
        Returns:
           True if equal, False otherwise
        
        """
        if (len(self.schema) != len(table.schema)):
            return False
        if (len(self.data) != len(table.data)):
            return False
        for i in range(len(self.schema)):
            if (self.schema[i] != table.schema[i]):
                return False
        for i in range(len(self.data)):
            if (self.data[i] != table.data[i]):
                return False
        if names_must_match:
            return self.name == table.name
        return True

    #
    # Check that a schema expressed as a list of tuples (name, type)
    # matches a list of rows given as data.  We let gviz_api do the 
    # checking for us.
    # Schema is a list of pairs [(<column_name>, <column_type>)]
    # where column_type is one of GALYLEO_STRING, GALYLEO_NUMBER, GALYLEO_BOOLEAN,
    # GALYLEO_DATE, GALYLEO_DATETIME, GALYLEO_TIME_OF_DAY.  All of these are defined
    # in galyleoconstants.  data is a list of lists, where each list is a row of 
    # the table.  Two conditions:
    # (1) Each type must be one of types listed above
    # (2) Each list in data must have the same length as the schema, and the type of each
    #     element must match the corresponding schema type
    # throws an InvalidDataException if either of these are violeated
    # parameters:
    #     schema: the schema as a list of pairs
    #     data: the data as a list of lists
    #
    def _check_schema_match(self, schema, data):
        for row in data:
            if (len(row) != len(schema)):
                raise InvalidDataException(f"All rows must have length {len(schema)}")
        try:
            table = gviz_api.DataTable(schema)
            table.LoadData(data)
            table.ToJSon()
        except gviz_api.DataTableException as schema_error:
            raise InvalidDataException(schema_error)

    
    def load_from_schema_and_data(self, schema:list, data:list):
        """     
        Load from a pair (schema, data).
        Schema is a list of pairs [(<column_name>, <column_type>)]
        where column_type is one of the Galyleo types (GALYLEO_STRING, GALYLEO_NUMBER, GALYLEO_BOOLEAN,
        GALYLEO_DATE, GALYLEO_DATETIME, GALYLEO_TIME_OF_DAY).  All of these are defined
        in galyleo_constants.  data is a list of lists, where each list is a row of 
        the table.  Two conditions:

        (1) Each type must be one of types listed above

        (2) Each list in data must have the same length as the schema, and the type of each
            element must match the corresponding schema type

        throws an InvalidDataException if either of these are violated

        Args:
            schema (list of pairs, (name, type)): the schema as a list of pairs
            data (list of lists): the data as a list of lists

        """
        self._check_schema_match(schema, data)
        self.schema = [{"name": record[0], "type": record[1]} for record in schema]
        self.data = data # should I clone?

    #
    # An internal routine used to map a Pandas or Numpy type to a Galyleo
    # type: mostly this involves mapping one of Numpy's many number types
    # to GALYLEO_NUMBER.  Used by load_from_dataframe.  If a type is unrecognized
    # it maps to GALYLEO_STRING
    # parameters:
    #   dtype: a Numpy or Pandas primitive type
    # returns: a Galyleo type
    #

    def _match_type(self, dtype):
        type_map = {
            GALYLEO_BOOLEAN: [numpy.bool_],
            GALYLEO_NUMBER:[ numpy.byte, numpy.ubyte, numpy.short, numpy.ushort, numpy.intc, numpy.uintc, numpy.int_, numpy.uint, numpy.longlong, numpy.ulonglong, numpy.float16, numpy.single, numpy.double, numpy.longdouble, numpy.csingle, numpy.cdouble, numpy.clongdouble]
        }
        for galyleo_type in type_map.keys():
            if dtype in type_map[galyleo_type]:
                return galyleo_type
        return GALYLEO_STRING

    def load_from_dataframe(self, dataframe, schema = None):
        """    
        Load from a Pandas Dataframe.  The schema is given in the optional second parameter,
        as a list of records {"name": <name>, "type": <type>}, where type is a Galyleo type. (GALYLEO_STRING, GALYLEO_NUMBER, GALYLEO_BOOLEAN,
        GALYLEO_DATE, GALYLEO_DATETIME, GALYLEO_TIME_OF_DAY). 
        If the second parameter is not present, the schema is derived from the name and
        column types of the dataframe, and each row of the dataframe becomes a row
        of the table.  

        Args:

            dataframe (pandas dataframe): the pandas dataframe to load from
            schema (list of dictionaries): if present, the schema in list of dictionary form; each dictionary is of the form {"name": <column name>, "type": <column type>}

        """
        if schema:
            self.schema = schema
        else:
            given_types = dataframe.dtypes
            galyleo_types = [self._match_type(dtype) for dtype in given_types]
            names = dataframe.columns
            self.schema = [{"name": names[i], "type": galyleo_types[i]} for i in range(len(names))]
        rows = [r for r in dataframe.iterrows()]
        self.data = [r[1].tolist() for r in rows]
    
   
    def as_dictionary(self):
        """     
        Return the form of the table as a dictionary.  This is a dictionary
        of the form:
        {"name": <table_name>,"table": <table_struct>} 
        where table_struct is of the form:
        {"columns": [<list of schema records],"rows": [<list of rows of the table>]}

        A schema record is a record of the form:
        {"name": < column_name>, "type": <column_type}, where type is one of the 
        Galyleo types (GALYLEO_STRING, GALYLEO_NUMBER, GALYLEO_BOOLEAN,
        GALYLEO_DATE, GALYLEO_DATETIME, GALYLEO_TIME_OF_DAY).  All of these are defined
        in galyleo_constants.

        Args:
            None

        Returns:
            {"name": <table_name>, "table": {"columns": <list of schema records], "rows": [<list of rows of the table>]}}

        """
        return {"name": self.name, "table": {"columns": self.schema, "rows": self.data}}

    
    def load_from_dictionary(self, dict):
        """     
        load data from a dictionary of the form: {"columns": [<list of schema records], "rows": [<list of rows of the table>]}

        A schema record is a record of the form:
        {"name": < column_name>, "type": <column_type}, where type is one of the 
        Galyleo types (GALYLEO_STRING, GALYLEO_NUMBER, GALYLEO_BOOLEAN,
        GALYLEO_DATE, GALYLEO_DATETIME, GALYLEO_TIME_OF_DAY).  
        
        Throws InvalidDataException if the dictionary is of the wrong format
        or the rows don't match the columns.

        Args:
            dict: the table as a dictionary (a value returned by as_dictionary)

        Throws:
           InvalidDataException if dict is malformed

        """
        self._check_fields(dict, {"columns", "rows"}, 'JSON  table descriptor')
        columns = dict["columns"]
        for column in columns:
            self._check_fields(column, {"name", "type"}, "Column description")
        schema = [(record["name"], record["type"]) for record in columns]
        self._check_schema_match(schema, dict["rows"])
        self.schema = columns
        self.data = dict["rows"]


    
    def to_json(self):
        """     
        Return the table as a JSON string, suitable for transmitting as a message
        or saving to a file.  This is just a JSON form of the dictionary form of
        the string.  (See as_dictionary)

        Returns:
           as_dictionary() as a JSON string
        
        """
        return dumps(self.as_dictionary())

    #
    # A utility to check if a dictionary contains all required keys
    # Raises an InvalidDataException if any are missing, with the 
    # appropriate error message
    # parameters:
    #   record: the record (dictionary) to be checked
    #   required_fields: the fields that must be in the record
    #   message_header: the phrase that must be in the message
    # 
    def _check_fields(self, record, required_fields, message_header):
        fields = set(record.keys())
        if (not fields.issuperset(required_fields)):
            raise InvalidDataException(f'{message_header} is missing fields {required_fields - fields}')
    
    def from_json(self, json_form, overwrite_name = True):
        """     
        Load the table from a JSON string, of the form produced by toJSON().  Note
        that if the overwrite_name parameter = True (the default), this will also
        overwrite the table name.

        Throws InvalidDataException id json_form is malformed

        Args:
            json_form: A JSON form of the Dictionary

        Returns:
            None

        Throws:
            InvalidDataException if json_form is malformed

        """
        try:
            record = loads(json_form)
        except JSONDecodeError(msg):
            raise InvalidDataException(msg)
        if (type(record) != dict):
            raise InvalidDataException(f'JSON form of table must be a dictionary, not {type(record)}')
        self._check_fields(record, {"name", "table"}, 'JSON form of table')
        self.load_from_dictionary(record["table"])
        if (overwrite_name):
            self.name = record["name"]
        
    def aggregate_by(self, aggregate_column_names, new_column_name = "count", new_table_name = None):
        """
        Create a new table by aggregating over multiple columns.  The resulting table
        contains the aggregate column names and the new column name, and for each
        unique combination of values among the aggregate column names, the count of rows in this
        table with that unique combination of values.
        The new table will have name new_table_name
        Throws an InvalidDataException if aggregate_column_names is not a subset of the names in self.schema

        Args:
            aggregate_column_names: names of the  columns to aggregate over
            new_column_name: name of the column for the aggregate count.  Defaults to count
            new_table_name: name of the new table.  If omitted, defaults to None, in which case a name will be generated

        Returns:
            A new table with name new_table_name, or a generated name if new_table_name == None
        
        Throws:
            InvalidDataException if one of the column names is missing
        
        """
        if (aggregate_column_names == None or len(aggregate_column_names) == 0):
             raise InvalidDataException('No columns specified for aggregation')
        column_names = set(aggregate_column_names)
        columns = [entry for entry in self.schema if entry["name"] in column_names]
        if (len(aggregate_column_names) != len(columns)):
            # We have a missing column.  Find it and throw the InvalidDataException
            current_columns = set([entry["name"] for entry in columns])
            missing_columns = column_names - current_columns
            raise InvalidDataException(f'Columns {missing_columns} are not present in the schema')
        # Make a table name
        if (new_table_name == None):
            letters = [name[0] for name in aggregate_column_names]
            new_table_name = 'aggregate_' + ''.join([name[0] for name in aggregate_column_names])
        # Collect the indices of the requested columns
        indices = [i  for i in range(len(self.schema)) if self.schema[i]["name"] in column_names]
        # Collect the columns for each row, making each short_row a tuple so they can be
        # indexed in a set
        simple_keys = len(indices) == 1
        if simple_keys:
            short_rows = [row[indices[0]] for row in self.data]
        else:
            short_rows = [tuple([row[i] for i in indices]) for row in self.data]
        keys = set(short_rows)
        # Now that we have the unique keys, count the number of instances of each
        count = {}
        for key in keys: count[key] = 0
        for key in short_rows: count[key] = count[key] + 1
        # convert the keys from tuples to lists, add the count for each one, and
        # filter out the 0's
        data = []
        for key in keys:
            key_as_list = [key] if simple_keys else list(key)
            data.append(key_as_list + [count[key]])
        data = [row for row in data if row[-1] > 0]
        # The schema is just the requested columns + new_column_name, and the type
        # of new_column_name is a number.  Then create the result table, load in the 
        # schema and data, and quit.
        schema = columns[:] + [{"name": new_column_name, "type": GALYLEO_NUMBER}]
        table = GalyleoTable(new_table_name)
        table.load_from_dictionary({"columns": schema, "rows": data})
        return table


    def filter_by_function(self, column_name, function, new_table_name, column_types = {}):
        '''
        Create a new table, with name table_name, with rows such that 
        function(row[column_name]) == True.  The new table will have
        columns {self.columns} - {column_name}, same types, and same order
        Throws an InvalidDataException if:
        1. new_table_name is None or not a string
        2. column_name is not a name of an existing column
        3. if column_types is not empty, the type of the selected column doesn't match one of the allowed types

        Args:
            column_name: the column to filter by
            function: a Boolean function with a single argument of the type of columns[column_name]
            new_table_name: name of the new table
            column_types: set of the allowed column types; if empty, any type is permitted
        
        Returns:
            A table with column[column_name] missing and filtered
        
        Throws:
            InvalidDataException if new_table_name is empty, column_name is not a name of an existing column, or the type of column_name isn't in column_types (if column_types is non-empty)
        '''
        if (not new_table_name ):
            raise InvalidDataException('new_table_name cannot be empty')
        if (not column_name):
            raise InvalidDataException('column_name cannot be empty')
        indices = [i for i in range(len(self.schema)) if self.schema[i]["name"] == column_name]
        if (len(indices) == 0):
            raise InvalidDataException(f'Column {column_name} not found in schema')
        index = indices[0]
        if (column_types):
            if (not self.schema[index]["type"] in column_types):
                raise InvalidDataException(f'Type {self.schema[index]["type"]} not found in {column_types}')
        data = [row[:index] + row[index+1:] for row in self.data if function(row[index])]
        schema = self.schema[:index] + self.schema[index+1:]
        result = GalyleoTable(new_table_name)
        result.load_from_dictionary({"columns": schema, "rows": data})
        return result

    def filter_equal(self, column_name, value, new_table_name, column_types):
        '''
        A convenience method over filter_by_function.  This is identical to
        filter_by_function(column_name, lambda x: x == value, new_table_name, column_types)

        Args:
            column_name: the column to filter by
            value: the value to march for equality
            new_table_name: name of the new table
            column_types: set of the allowed column types; if empty, any type is permitted
        
        Returns:
            A table with column[column_name] missing and filtered
        
         Throws:
            InvalidDataException if new_table_name is empty, column_name is not a name of an existing column, or the type of column_name isn't in column_types (if column_types is non-empty)
        '''

        return self.filter_by_function(column_name, lambda x: x == value, new_table_name, column_types)

    def filter_range(self, column_name, range_as_tuple, new_table_name, column_types):
        '''
        A convenience method over filter_by_function.  This is identical to
        filter_by_function(column_name, lambda x: x >= range_as_tuple[0], x <= range_as_tuple[1], new_table_name, column_types)

        Args:
            column_name: the column to filter by
            range_as_tuple: the tuple representing the range
            new_table_name: name of the new table
            column_types: set of the allowed column types; if empty, any type is permitted
        
        Returns:
            A table with column[column_name] missing and filtered
        
         Throws:
            InvalidDataException if new_table_name is empty, column_name is not a name of an existing column, or the type of column_name isn't in column_types (if column_types is non-empty), if len(range_as_tuple) != 2
        '''

        try:
            assert(range_as_tuple and len(range_as_tuple) == 2)
        except Exception:
            raise InvalidDataException(f'{range_as_tuple} should be a tuple of length 2')
        
        return self.filter_by_function(column_name, lambda x: x <= range_as_tuple[1] and x >= range_as_tuple[0], new_table_name, column_types)

    # 
    # A utility to get the index of a column, given a name.  Raises an InvalidDataException
    # is no such column in the schema
    #
    #   Args: 
    #     column_name: name of the column
    #   
    #   Returns:
    #     index of the column
    #
    #   Throws:
    #     InvalidDataException if there is no such column
    #

    def _get_column_index(self, column_name):
        indices = [i for i in range(len(self.schema)) if self.schema[i]["name"] == column_name]
        if (len(indices) == 0):
            raise InvalidDataException(f'Column {column_name} is not in the schema')
        return indices[0]
        
    def pivot_on_column(self, pivot_column_name, value_column_name, new_table_name, pivot_column_values = {}, other_column = False):
        '''
        The pivot_on_column method breaks out value_column into n separate columns, one for each
        member of pivot_column_values plus (if other_column = True), an "Other" column.  This is easiest to see with an example.  Consider a table with columns (Year, State, Party, Percentage).  pivot_on_column('Party', {'Republican', 'Democratic'}, 'Percentage', 'pivot_table', False) would create a new table with columns Year, State, Republican, Democratic, where the values in the Republican and Democratic columns are the  values in the Percentage column where the Party column value was Republican or Democratic, respectively.  If Other = True, an additional column, Other, is found where the value is (generally) the sum of values where Party not equal Republican or Democratic

        Args:
            pivot_column_name: the column holding the keys to pivot on
            value_column_name: the column holding the values to spread out over the pivots
            new_table_name: name of the new table
            pivot_column_values: the values to pivot on.  If empty, all values used
            other_column: if True, aggregate other values into a column
        
        Returns:
            A table as described in the comments above
        
         Throws:
            InvalidDataException if new_table_name is empty, pivot_column_name is not a name of an existing column, or value_column_name is not the name of an existing column
        '''
        names = [(new_table_name, 'new_table_name'), (pivot_column_name, 'pivot_column_name'), (value_column_name, 'value_column_name')]
        for name in names:
            if (not name[0]):
                raise InvalidDataException(f'{name[1]} cannot be empty')
        if (value_column_name == pivot_column_name):
            raise InvalidDataException(f'Pivot and value columns cannot be identical: both are {value_column_name}')
       
        value_column_index = self._get_column_index(value_column_name)
        pivot_column_index = self._get_column_index(pivot_column_name)
        key_columns = list(set(range(len(self.schema))) - {value_column_index, pivot_column_index})
        key_columns.sort()


        # Split each row into a dict: 
        # key (value of the other columns). Note this is a tuple so it can index a set
        # pivot_value: value of the pivot column
        # value: value of the value column


        def make_record(row):
            return {
                "key": tuple([row[i] for i in key_columns]),
                "pivot": row[pivot_column_index],
                "value": row[value_column_index]
            }
        
        partition = [make_record(row) for row in self.data]
            

        
        # get the set of all distinct keys
        keys = set([record["key"] for record in partition])

        # get the distinct values of the pivot column
        pivot_value_set = set([record["pivot"] for record in partition])

        # Note whether we will have an "Other" column.  We will when:
        # (a) other_column = True AND
        # (b) pivot_column_values is not empty AND
        # (c) there are columns in pivot_value_set - pivot_column_values
        other_columns = pivot_value_set - pivot_column_values if pivot_column_values else {}
        use_other = other_column and other_columns

        if (pivot_column_values):
            pivot_value_set = pivot_value_set.intersection(pivot_column_values)


        value_column_type = self.schema[value_column_index]["type"]
        
        def new_pivot_record():
            initial_value = 0 if value_column_type == GALYLEO_NUMBER else None
            result = {}
            for name in pivot_value_set: result[name] = initial_value
            result["Other"] = initial_value
            return result

        # set up the dictionary
        pivot_records = {}
        for key in keys: pivot_records[key] = new_pivot_record()
        for record in partition:
            pivot = record["pivot"]
            if (pivot in pivot_value_set): pivot_records[key][pivot] = record["value"]
            else: pivot_records[key]["Other"] = record["value"] + pivot_records[key]["Other"]
        
        # Now just create and return the new table
        new_schema = [self.schema[i] for i in key_columns]

        pivot_schema = [{"name": name, "type": value_column_type} for name in pivot_value_set]
        
        if (use_other):
            pivot_schema.append({"name": "Other", "type": value_column_type})
        
        def make_record(key_value):
            result = list(key_value)
            record = pivot_records[key_value]
            values = [record[pivot] for pivot in pivot_value_set]
            if (use_other): values.append(record["Other"])
            return result + values

        data = [make_record(key) for key in pivot_records]
        result = GalyleoTable(new_table_name)
        result.load_from_dictionary({"columns": new_schema + pivot_schema, "rows": data})
        return result




class RemoteGalyleoTable:
    '''
    A Remote Galyleo Table: This is instantiated with an URL which tells the 
    dashboard where to get the data.  Note that this is much simpler than an explicit
    GalyleoTable, since the data manipulation is all done by the remote server.
    '''
    def __init__(self, name:str, schema, base_url:str, header_variables = []):
        '''
        Initialize with the name, the schema (same as for an explicit table, above),
        the base_url, and the header_variables which are used for authemtication
        Arguments:
            name: string, name of the table
            schema: list of the form {"name", "type"} for each column, same as in the parent
            base_url: the base URL to fetch the data from, a string.  The dashboard will add arguments for the methods
                      The server must implement the Galyleo Data Protocol
            header_variables: a list of strings: header variables which are to be transmitted to the data server for authentication

        '''
        self.name = name
        self.schema = schema
        self.base_url = base_url
        self.header_variables = header_variables if header_variables else []
        self.data = [] # for compatibility with GalyleoTable -- so that client checks for data will be OK

    def as_dictionary(self):
        """     
        Return the form of the table as a dictionary.  This is a dictionary
        of the form:
        {"name": <table_name>,"table": <table_struct>} 
        where table_struct is of the form:
        {"columns": [<list of schema records], "base_url": base_url, "header_variables": list of header variables}

        A schema record is a record of the form:
        {"name": < column_name>, "type": <column_type}, where type is one of the 
        Galyleo types (GALYLEO_STRING, GALYLEO_NUMBER, GALYLEO_BOOLEAN,
        GALYLEO_DATE, GALYLEO_DATETIME, GALYLEO_TIME_OF_DAY).  All of these are defined
        in galyleo_constants.

        Args:
            None

        Returns:
            {"name": <table_name>, "table": {"columns": <list of schema records], "base_url": base_url, "header_variables": list of header variables}}

        """
        return {
            "name": self.name,
            "table": {
                "base_url": self.base_url,
                "columns": self.schema,
                "header_variables": self.header_variables
            }
        }
        
    def to_json(self):
        """     
        Return the table as a JSON string, suitable for transmitting as a message
        or saving to a file.  This is just a JSON form of the dictionary form of
        the string.  (See as_dictionary)

        Returns:
           as_dictionary() as a JSON string
        
        """
        return dumps(self.as_dictionary())

        
       



