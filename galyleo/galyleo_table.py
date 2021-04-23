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

'''A Galyleo Dashboard Table.  Used to create a Galyleo Dashboard Table from any of a number of sources, and then generate an object that is suitable
for storage (as a JSON file) '''

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
    def __init__(self, name:str):
        self.name = name
        self.schema = []
        self.data = []

#
# Test to see if this table is equal to another table, passed as
# an argument.  Two tables are equal if their schemas are the same
# length and column names and types match, and if the data is the same,
# and in the same order.  If names_must_match == True (default is False),
# then the names must also match
#  parameters:
#    table: table to be checked for equality
#    names_must_match: (default False) if True, table names must also match
#

    def equal(self, table, names_must_match = False):
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
    # matches a list of rows given as data.  We let gviz_api do teh 
    # checking for us.
    # Schema is a list of pairs [(<column_name>, <column_type>)]
    # where column_type is one of GALYLEO_STRING, GALYLEO_NUMBER, GALYLEO_BOOLEAN,
    # GALYLEO_DATE, GALYLEO_DATETIME, GALYLEO_TIME_OF_DAY.  All of these are defined
    # in livelyconst.  data is a list of lists, where each list is a row of 
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


    #
    # Load from a pair (schema, data).
    # Schema is a list of pairs [(<column_name>, <column_type>)]
    # where column_type is one of GALYLEO_STRING, GALYLEO_NUMBER, GALYLEO_BOOLEAN,
    # GALYLEO_DATE, GALYLEO_DATETIME, GALYLEO_TIME_OF_DAY.  All of these are defined
    # in livelyconst.  data is a list of lists, where each list is a row of 
    # the table.  Two conditions:
    # (1) Each type must be one of types listed above
    # (2) Each list in data must have the same length as the schema, and the type of each
    #     element must match the corresponding schema type
    # throws an InvalidDataException if either of these are violeated
    # parameters:
    #     schema: the schema as a list of pairs
    #     data: the data as a list of lists
    #
    
    def load_from_schema_and_data(self, schema:list, data:list):
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

   #
   # Load from a Pandas Dataframe.  The schema is given in the optional second parameter,
   # as a list of records {"name": <name>, "type": <type>}, where type is a Galyleo type. 
   # If the second parameter is not present, the schema is derived from the name and
   # column types of the dataframe, and each row of the dataframe becomes a row
   # of the table.  
   # parameters:
   #   dataframe: the pandas dataframe to load from
   #   schema: if present, the schema in list of dictionary form
   #

    
    def load_from_dataframe(self, dataframe, schema = None):
        if schema:
            self.schema = schema
        else:
            given_types = dataframe.dtypes
            galyleo_types = [self._match_type(dtype) for dtype in given_types]
            names = dataframe.columns
            self.schema = [{"name": names[i], "type": galyleo_types[i]} for i in range(len(names))]
        rows = [r for r in dataframe.iterrows()]
        self.data = [r[1].tolist() for r in rows]
    
    #
    # Return the form of the table as a dictionary.  This is a dictionary
    # of the form:
    # {"name": <table_name>,
    # "table": {
    #     "columns": [<list of schema records],
    #     "rows": [<list of rows of the table>]
    #   }
    # }
    # A schema record is a record of the form:
    # {"name": < column_name>, "type": <column_type}, where type is one of the 
    # Galyleo types
    # 
    def as_dictionary(self):
        return {"name": self.name, "table": {"columns": self.schema, "rows": self.data}}
    
    #
    # load data from a dictionary of the form 
    #     "columns": [<list of schema records],
    #     "rows": [<list of rows of the table>]
    #   }
    # }
    # A schema record is a record of the form:
    # {"name": < column_name>, "type": <column_type}, where type is one of the 
    # Galyleo types
    # 
    # Throws InvalidDataException if the dictionary is of the wrong format
    # or the rows don't match the columns
    # parameters
    #   dict: the table as a dictionary
    # 
    def load_from_dictionary(self, dict):
        self._check_fields(dict, {"columns", "rows"}, 'JSON  table descriptor')
        columns = dict["columns"]
        for column in columns:
            self._check_fields(column, {"name", "type"}, "Column description")
        schema = [(record["name"], record["type"]) for record in columns]
        self._check_schema_match(schema, dict["rows"])
        self.schema = columns
        self.data = dict["rows"]

    #
    # Return the table as a JSON string, suitable for transmitting as a message
    # or saving to a file.  This is just a JSON form of the dictionary form of
    # the string
    # 
    def to_json(self):
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

    #
    # Load the table from a JSON string, of the form produced by toJSON().  Note
    # that if the overwrite_name parameter = True (the default), this will also
    # overwrite the table name.
    # Throws InvalidDataException id json_form is malformed
    # parameters:
    #   json_form: the JSON string to turn into a Table
    #
    
    def from_json(self, json_form, overwrite_name = True):
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
        
            
