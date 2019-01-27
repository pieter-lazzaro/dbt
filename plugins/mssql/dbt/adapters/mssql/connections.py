from contextlib import contextmanager

import pyodbc

import dbt.compat
import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.logger import GLOBAL_LOGGER as logger


MSSQL_CREDENTIALS_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'database': {
            'type': 'string',
        },
        'host': {
            'type': 'string',
        },
        'user': {
            'type': 'string',
        },
        'password': {
            'type': 'string',
        },
        'port': {
            'type': 'integer',
            'minimum': 0,
            'maximum': 65535,
        },
        'schema': {
            'type': 'string',
        },
        'keepalives_idle': {
            'type': 'integer',
        },
    },
    'required': ['database', 'host', 'user', 'password', 'port', 'schema'],
}


class MssqlCredentials(Credentials):
    SCHEMA = MSSQL_CREDENTIALS_CONTRACT
    ALIASES = {
        'dbname': 'database',
        'pass': 'password'
    }

    @property
    def type(self):
        return 'mssql'

    def _connection_keys(self):
        return ('host', 'port', 'user', 'database', 'schema')


class MssqlConnectionManager(SQLConnectionManager):
    DEFAULT_TCP_KEEPALIVE = 0  # 0 means to use the default value
    TYPE = 'mssql'

    @contextmanager
    def exception_handler(self, sql, connection_name='master'):
        try:
            yield

        except pyodbc.DatabaseError as e:
            logger.debug('MSSQL error: {}'.format(str(e)))

            try:
                # attempt to release the connection
                self.release(connection_name)
            except pyodbc.Error:
                logger.debug("Failed to release connection!")
                pass

            raise dbt.exceptions.DatabaseException(
                dbt.compat.to_string(e).strip())

        except Exception as e:
            logger.debug("Error running SQL: %s", sql)
            logger.debug("Rolling back transaction.")
            self.release(connection_name)
            raise dbt.exceptions.RuntimeException(e)

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        base_credentials = connection.credentials
        credentials = cls.get_credentials(connection.credentials.incorporate())
        kwargs = {}
        keepalives_idle = credentials.get('keepalives_idle',
                                          cls.DEFAULT_TCP_KEEPALIVE)
        # we don't want to pass 0 along to connect() as postgres will try to
        # call an invalid setsockopt() call (contrary to the docs).
        if keepalives_idle:
            kwargs['keepalives_idle'] = keepalives_idle

        try:
            handle = pyodbc.connect(
                driver='{ODBC Driver 17 for SQL Server}',
                server=credentials.server,
                uid=credentials.user,
                pwd=credentials.password,
                timeout=10,
                autocommit=False,
                **kwargs)

            connection.handle = handle
            connection.state = 'open'
        except pyodbc.Error as e:
            logger.debug("Got an error when attempting to open a mssql "
                         "connection: '{}'"
                         .format(e))

            connection.handle = None
            connection.state = 'fail'

            raise dbt.exceptions.FailedToConnectException(str(e))

        return connection

    def cancel(self, connection):
        # there is no way to cancel connections, only queries can be cancelled if
        # you have the cursor. I am not sure if it is worth wrapping the connection
        # object or not
        pass

    def begin(self, name): 
        # No need to start transactions: 
        # https://github.com/mkleehammer/pyodbc/wiki/Database-Transaction-Management
        pass
    
    def commit(self, connection):
        
        if dbt.flags.STRICT_MODE:
            assert isinstance(connection, Connection)

        connection = self.get(connection.name)

        logger.debug('On {}: COMMIT'.format(connection.name))
        
        try:
            connection.handle.commit()
        except pyodbc.Error as e:
            logger.debug("Got an error when attempting commit "
                         "transaction: '{}'"
                         .format(e))
        self.in_use[connection.name] = connection

        return connection

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    @classmethod
    def get_status(cls, cursor):
        # ODBC doesn't have a status attribute
        return 'OK'
