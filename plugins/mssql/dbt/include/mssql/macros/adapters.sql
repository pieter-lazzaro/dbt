
{% macro mssql__create_schema(database_name, schema_name) -%}
  {% set schema = schema_name.strip('[]') %}
  {% if database_name -%}
    {{ adapter.verify_database(database_name) }}
  {%- endif -%}
  {%- call statement('create_schema') -%}
    if not exists (select name from sys.schemas where name = '{{ schema }}') 
    begin
      exec sp_executesql N'create schema {{ schema_name }}'
    end
  {%- endcall -%}
{% endmacro %}

{% macro mssql__drop_schema(database_name, schema_name) -%}
  {% if database_name -%}
    {{ adapter.verify_database(database_name) }}
  {%- endif -%}
  {%- call statement('drop_schema') -%}
/********************************************************
 COPYRIGHTS http://www.ranjithk.com
*********************************************************/ 
declare @SchemaName varchar(100) = '{{schema_name}}'
declare @WorkTest char(1) = 'w'  -- use 'w' to work and 't' to print
declare @dropSchema BIT = 1
/*-----------------------------------------------------------------------------------------
 
  Author : Ranjith Kumar S
  Date:    31/01/10
 
  Description: It drop all the objects in a schema and then the schema itself
 
  Limitations:
  
    1. If a table has a PK with XML or a Spatial Index then it wont work
       (workaround: drop that table manually and re run it)
    2. If the schema is referred by a XML Schema collection then it wont work
 
If it is helpful, Please send your comments ranjith_842@hotmail.com or visit http://www.ranjithk.com
 
-------------------------------------------------------------------------------------------*/
declare @SQL varchar(4000)
declare @msg varchar(500)
 
IF OBJECT_ID('tempdb..#dropcode') IS NOT NULL DROP TABLE #dropcode
CREATE TABLE #dropcode
(
   ID int identity(1,1)
  ,SQLstatement varchar(1000)
 )
 
-- removes all the foreign keys that reference a PK in the target schema
 SELECT @SQL =
  'select
       '' ALTER TABLE ''+SCHEMA_NAME(fk.schema_id)+''.''+OBJECT_NAME(fk.parent_object_id)+'' DROP CONSTRAINT ''+ fk.name
  FROM sys.foreign_keys fk
  join sys.tables t on t.object_id = fk.referenced_object_id
  where t.schema_id = schema_id(''' + @SchemaName+''')
    and fk.schema_id <> t.schema_id
  order by fk.name desc'
 
 IF @WorkTest = 't' PRINT (@SQL )
 INSERT INTO #dropcode
 EXEC (@SQL)

-- drop all default constraints, check constraints and Foreign Keys
SELECT @SQL =
'SELECT
      '' ALTER TABLE ''+schema_name(t.schema_id)+''.''+OBJECT_NAME(fk.parent_object_id)+'' DROP CONSTRAINT ''+ fk.[Name]
 FROM sys.objects fk
 join sys.tables t on t.object_id = fk.parent_object_id
 where t.schema_id = schema_id(''' + @SchemaName+''')
  and fk.type IN (''D'', ''C'', ''F'')'
  
IF @WorkTest = 't' PRINT (@SQL )
INSERT INTO #dropcode
EXEC (@SQL)
 
 -- drop all other objects in order   
 SELECT @SQL =  
 'SELECT
      CASE WHEN SO.type=''PK'' THEN '' ALTER TABLE ''+SCHEMA_NAME(SO.schema_id)+''.''+OBJECT_NAME(SO.parent_object_id)+'' DROP CONSTRAINT ''+ SO.name
           WHEN SO.type=''U'' THEN '' DROP TABLE ''+SCHEMA_NAME(SO.schema_id)+''.''+ SO.[Name]
           WHEN SO.type=''V'' THEN '' DROP VIEW  ''+SCHEMA_NAME(SO.schema_id)+''.''+ SO.[Name]
           WHEN SO.type=''P'' THEN '' DROP PROCEDURE  ''+SCHEMA_NAME(SO.schema_id)+''.''+ SO.[Name]         
           WHEN SO.type=''TR'' THEN ''  DROP TRIGGER  ''+SCHEMA_NAME(SO.schema_id)+''.''+ SO.[Name]
		   WHEN SO.type=''SN'' THEN '' DROP SYNONYM ''+SCHEMA_NAME(SO.schema_id)+''.''+ SO.[Name]
           WHEN SO.type= ''SO'' THEN '' DROP SEQUENCE ''+SCHEMA_NAME(SO.schema_id)+''.''+ SO.[Name]
           WHEN SO.type  IN (''FN'', ''TF'',''IF'',''FS'',''FT'') THEN '' DROP FUNCTION  ''+SCHEMA_NAME(SO.schema_id)+''.''+ SO.[Name]
       END
FROM SYS.OBJECTS SO
WHERE SO.schema_id = schema_id('''+ @SchemaName +''')
  AND SO.type IN (''PK'', ''FN'', ''TF'', ''TR'', ''V'', ''U'', ''P'', ''SN'', ''IF'',''SO'')
ORDER BY CASE WHEN type = ''PK'' THEN 1
              WHEN type in (''FN'', ''TF'', ''P'',''IF'',''FS'',''FT'') THEN 2
              WHEN type = ''TR'' THEN 3
              WHEN type = ''V'' THEN 4
              WHEN type = ''U'' THEN 5
            ELSE 6
          END'
 
IF @WorkTest = 't' PRINT (@SQL )
INSERT INTO #dropcode
EXEC (@SQL)
 
DECLARE @ID int, @statement varchar(1000)
DECLARE statement_cursor CURSOR
FOR SELECT SQLStatement
      FROM #dropcode
  ORDER BY ID ASC
    
 OPEN statement_cursor
 FETCH statement_cursor INTO @statement
 WHILE (@@FETCH_STATUS = 0)
 BEGIN
 
 IF @WorkTest = 't' PRINT (@statement)
 ELSE
  BEGIN
    PRINT (@statement)
    EXEC(@statement)
  END
  
 FETCH statement_cursor INTO @statement    
END
 
CLOSE statement_cursor
DEALLOCATE statement_cursor
 
IF @dropSchema = 1
BEGIN
   IF @WorkTest = 't' PRINT ('DROP SCHEMA '+@SchemaName)
   ELSE
   BEGIN
      PRINT ('DROP SCHEMA '+@SchemaName)
      EXEC ('DROP SCHEMA '+@SchemaName)
	END
END

  {%- endcall -%}
{% endmacro %}

{% macro mssql__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
          column_name,
          data_type,
          character_maximum_length,
          numeric_precision,
          numeric_scale

      from {{ information_schema_name(relation.database) }}.columns
      where table_name = '{{ relation.identifier }}'
        {% if relation.schema %}
        and table_schema = '{{ relation.schema }}'
        {% endif %}
      order by ordinal_position

  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}


{% macro mssql__list_relations_without_caching(database, schema) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    SELECT 
      '{{ database }}' as [database],
      tables.name as name,
      schemas.name as [schema],
      'table' as type
    FROM [sys].[tables]
    JOIN sys.schemas on sys.tables.schema_id=sys.schemas.schema_id
    WHERE schemas.name = '{{ schema }}' COLLATE SQL_Latin1_General_CP1_CI_AS
    union all
    SELECT 
      '{{ database }}' as [database],
      views.name as name,
      schemas.name as [schema],
      'view' as type
    FROM [sys].views
    JOIN sys.schemas on sys.views.schema_id=sys.schemas.schema_id
    WHERE schemas.name = '{{ schema }}' COLLATE SQL_Latin1_General_CP1_CI_AS
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro mssql__information_schema_name(database) -%}
  {% if database_name -%}
    {{ adapter.verify_database(database_name) }}
  {%- endif -%}
  information_schema
{%- endmacro %}

{% macro mssql__list_schemas(database) %}
  {% if database -%}
    {{ adapter.verify_database(database) }}
  {%- endif -%}
  select distinct name from sys.schemas
{% endmacro %}

{% macro mssql__check_schema_exists(database, schema) -%}
  {% if database -%}
    {{ adapter.verify_database(database) }}
  {%- endif -%}
  {% call statement('check_schema_exists', fetch_result=True, auto_begin=False) %}
    select count(*) from sys.schemas where name = '{{ schema }}'
  {% endcall %}
{% endmacro %}


{% macro mssql__alter_column_type(relation, column_name, new_column_type) -%}
  {% call statement('alter_column_type') %}
    alter table {{ relation }} alter column {{ column_name }} {{ new_column_type }};
  {% endcall %}

{% endmacro %}


{% macro mssql__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    if object_id('{{ relation }}') is not null begin
      drop {{ relation.type }} {{ relation }}
    end
  {%- endcall %}
{% endmacro %}

{% macro mssql__rename_relation(from_relation, to_relation) -%}
  {% set to_identifier = to_relation.identifier %}
  {% set to_schema = adapter.quote_as_configured(to_relation.schema, 'schema') %}
  {% set from_schema = adapter.quote_as_configured(from_relation.schema, 'schema') %}
  {% call statement('rename_relation') -%}
    EXEC sp_rename '{{ from_relation }}', '{{ to_identifier }}';
    {% if from_relation.schema != to_relation.schema: %}}
    alter schema {{ from_schema }} transfer {{ to_schema }};
    {% endif %}
  {%- endcall %}
{% endmacro %}


{% macro mssql__create_table_as(temporary, relation, sql) -%}
  {# TODO: handle temporary tables #}
  {% set ctes = adapter.extract_ctes(sql) %}
  {% set query = adapter.extract_query(sql) %}
  {{ ctes }}
  SELECT * INTO {{ relation.include(database=(not temporary), schema=(not temporary)) }}
  FROM (
    {{ query }}
  ) as create_table_as;
{% endmacro %}


{% macro mssql__create_view_as(relation, sql) -%}
  create view {{ relation }} as 
    {{ sql }}
  ;
{% endmacro %}
