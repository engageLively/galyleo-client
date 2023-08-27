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

"""
Constants that are used throughout the module.  These include:
   1. Data types for a table (GALYLEO_STRING, GALYLEO_NUMBER, GALYLEO_BOOLEAN, GALYLEO_DATE,
      GALYLEO_DATETIME, GALYLEO_TIME_OF_DAY)
   2. GALYLEO_TYPES: The types in a list
   3. MAXIMUM_DATA_SIZE: Maximum size, in bytes, of a GalyleoTable
   4. MAX_TABLE_ROWS: Maximum number of rows in a GalyleoTable
"""

LIBRARY_VERSION = "2021.x.y"

GALYLEO_STRING = 'string'
GALYLEO_NUMBER = 'number'
GALYLEO_BOOLEAN = 'boolean'
GALYLEO_DATE = 'date'
GALYLEO_DATETIME = 'datetime'
GALYLEO_TIME_OF_DAY = 'timeofday'

""" Types for a chart/dashboard table schema """
GALYLEO_SCHEMA_TYPES = ['string', 'number', 'boolean', 'date', 'datetime','timeofday']

""" Maximum size of a table sent to the dashboard.  Exceeding this will throw a DataSizeExceeded exception """
MAX_DATA_SIZE = 1*2**24

"""Maximum number of rows in a table"""
MAX_TABLE_ROWS = 1000000  # 1 million rows per table  at most.

# Other constants
MILLISECONDS_PER_SECOND = 1000
