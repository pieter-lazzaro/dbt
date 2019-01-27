
{% macro mssql__get_catalog() -%}

  {%- call statement('catalog', fetch_result=True) -%}
    {% if (databases | length) != 1 %}
        exceptions.raise_compiler_error('mssql get_catalog requires exactly one database')
    {% endif %}
    {% set database = databases[0] %}
    {{ adapter.verify_database(database) }}

    with table_owners as (

        SELECT
            '{{ database }}' as [table_database],
            sys.schemas.name as [table_schema],
            sys.tables.name as [table_name],
            USER_NAME(coalesce(tables.[principal_id], schemas.[principal_id])) as [table_owner]
        FROM sys.tables
        JOIN sys.schemas on sys.tables.schema_id=sys.schemas.schema_id


        union all

        SELECT
            '{{ database }}' as table_database,
            sys.schemas.name as table_schema,
            sys.views.name as table_name,
            USER_NAME(coalesce(views.[principal_id], schemas.principal_id)) as table_owner
        FROM sys.views
        JOIN sys.schemas on sys.views.schema_id=sys.schemas.schema_id


    ),

    tables as (

        select
            table_catalog as [table_database],
            table_schema as [table_schema],
            table_name as [table_name],
            table_type as [table_type]

        from information_schema.tables

    ),

    columns as (

        select
            table_catalog as [table_database],
            table_schema as [table_schema],
            table_name as [table_name],
            null as table_comment,
            column_name as [column_name],
            ordinal_position as column_index,
            data_type as column_type,
            null as column_comment

        from information_schema.columns

    )

    select 
		*
    from tables
    join columns on (
			tables.table_database = columns.table_database 
		and tables.table_schema = columns.table_schema 
		and tables.table_name = columns.table_name
	)
    join table_owners on (
			tables.table_database = table_owners.table_database 
		and tables.table_schema = table_owners.table_schema 
		and tables.table_name = table_owners.table_name
	)

	order by columns.column_index


  {%- endcall -%}

  {{ return(load_result('catalog').table) }}

{%- endmacro %}
