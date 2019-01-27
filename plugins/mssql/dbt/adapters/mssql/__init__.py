from dbt.adapters.mssql.connections import MssqlConnectionManager
from dbt.adapters.mssql.connections import MssqlCredentials
from dbt.adapters.mssql.impl import MssqlAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import mssql

Plugin = AdapterPlugin(
    adapter=MssqlAdapter,
    credentials=MssqlCredentials,
    include_path=mssql.PACKAGE_PATH)
