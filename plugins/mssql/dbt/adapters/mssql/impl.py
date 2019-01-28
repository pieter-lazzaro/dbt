import psycopg2

import time

from dbt.adapters.base.meta import available_raw
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.mssql import MssqlConnectionManager
import dbt.compat
import dbt.exceptions
import agate

from dbt.logger import GLOBAL_LOGGER as logger


# note that this isn't an adapter macro, so just a single underscore
GET_RELATIONS_MACRO_NAME = 'mssql_get_relations'


class MssqlAdapter(SQLAdapter):
    ConnectionManager = MssqlConnectionManager

    @classmethod
    def date_function(cls):
        return 'getdate()'

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        return "ntext"

    @classmethod
    def convert_number_type(cls, agate_table, col_idx):
        # TODO: determine maximum value and size type appropriately
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        if decimals:
            return "int"
        return "float(8)"

    @classmethod
    def convert_boolean_type(cls, agate_table, col_idx):
        return "bit"

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx):
        return "datetime2"

    @classmethod
    def convert_date_type(cls, agate_table, col_idx):
        return "date"

    @classmethod
    def convert_time_type(cls, agate_table, col_idx):
        return "time"

    @classmethod
    def quote(cls, identifier):
        return '[{}]'.format(identifier)

    @available_raw
    def verify_database(self, database):
        database = database.strip('[]')
        expected = self.config.credentials.database
        if database != expected:
            raise dbt.exceptions.NotImplementedException(
                'Cross-db references not allowed in {} ({} vs {})'
                .format(self.type(), database, expected)
            )
        # return an empty string on success so macros can call this
        return ''
