IF OBJECT_ID('dbo.quick_update_stats') IS NULL
  EXEC ('CREATE PROCEDURE dbo.quick_update_stats AS RETURN 0;');
GO

ALTER PROCEDURE [dbo].[quick_update_stats]
( 
	@database sysname,
	@sql_object sysname,
	@fullscan int=0,
	@maxdop int =0
)
/*============================================================================
  File:     quick_update_stats.sql
  
  Summary:  Generate script for update statistics on SQL Server.
  
  SQL Server Versions: 2014 onwards
------------------------------------------------------------------------------
 Written by Pavel A. Polikov https://github.com/PahanDba/mssql_dba
 Create a script to update the statistics of the objects that are used in the procedure. 
 Objects that are used in the 1st level procedures in the source procedure are taken into account.
 Example, procedure prots1 is started. Inside the procedure, there is work with different tables: table1, table2, table3, 
 and procedure prots2 is called. Inside the procedure proc2, work is being done with the tables table4, table5, table6, 
 and the procedure proc3 is called. Inside procedure proc3, work is being done with tables table7, table8, table9. 
 The script will generate an update of each statistics for tables table1, table2, table3, table4, table5, table6.
 Procedure objects proc3 are not processed. 
 The script is created without incremental statistics, without partitioning.
 You can specify to update statistics with fullscan.   

   Example: 
   exec dbo.quick_update_stats @database='sqlnexus', @sql_object='[dbo].[test1]' ,@fullscan=0, @maxdop=1
   exec dbo.quick_update_stats @database='sqlnexus', @sql_object='[dbo].[test1]' ,@fullscan=1, @maxdop=1
   exec dbo.quick_update_stats @database='StackOverflow', @sql_object='[dbo].[test1]' ,@fullscan=0, @maxdop=1
  You may alter this code for your own *non-commercial* purposes (e.g. in a
  for-sale commercial tool). USE in your own environment is encouraged.
  You may republish altered code as long as you include this copyright and
  give due credit, but you must obtain prior permission before blogging
  this code.
  
  THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF
  ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED
  TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
  PARTICULAR PURPOSE.

============================================================================*/
as	
set nocount on;
begin
declare	@sqlstrall nvarchar(4000),
		@schema nvarchar(300),
		@tbl nvarchar(300),
		@stat NVARCHAR(300),
		@obj  NVARCHAR(300),
		@server_collation nvarchar(128),
		@server_name nvarchar(128),
		@database_collation nvarchar(128),
		@ErrorMessage nvarchar(4000),  
		@ErrorSeverity int,
		@ErrorState INT;
set @server_name=(select @@SERVERNAME)
set @server_collation=(cast ( SERVERPROPERTY ('Collation') as nvarchar(128)))
set @database_collation=(cast ( DATABASEPROPERTYEX (@database,'Collation') as nvarchar(128)))
if @database_collation!=@server_collation
	begin
		set @ErrorMessage ='Database collation '+@database+' is '+@database_collation+'. The SQL Server collation is '+@server_collation+'. At the moment, if the SQL Server and database collation are different, the procedure does not work'
			RAISERROR (@ErrorMessage, -- Message text.  
					   16, -- Severity.  
					   1 -- State.  
					   );  
		return 1
	end

if @fullscan>=1 set @fullscan=1

IF OBJECT_ID('tempdb..#object_maintenance_start') IS NOT NULL DROP TABLE #object_maintenance_start;
create table #object_maintenance_start
(id int identity(1,1) primary key,
name_schema sysname,
name_object sysname,
name_desc sysname,
obj_id bigint
);
set @sqlstrall='SELECT re.referenced_schema_name, re.referenced_entity_name, ao.type_desc, ao.object_id
    FROM ['+@database+'].sys.dm_sql_referenced_entities ('''+@sql_object+''',''OBJECT'') re
JOIN ['+@database+'].sys.all_objects ao
ON schema_name (ao.schema_id) =re.referenced_schema_name AND ao.name=re.referenced_entity_name
WHERE re.referenced_minor_id=0
ORDER BY re.referenced_schema_name, re.referenced_entity_name'
insert into #object_maintenance_start(name_schema,name_object,name_desc,obj_id)
exec ( @sqlstrall)
--Table for collect objects  procedure 1 level
IF OBJECT_ID('tempdb..#object_maintenance_level1') IS NOT NULL DROP TABLE #object_maintenance_level1;
create table #object_maintenance_level1
(id int identity(1,1) primary key,
name_schema sysname,
name_object sysname,
name_desc sysname,
obj_id bigint
);
DECLARE sp_complete_cursor_level1  CURSOR FAST_FORWARD FOR 
select name_schema,name_object from #object_maintenance_start 
where name_desc in ('SQL_SCALAR_FUNCTION','CLR_SCALAR_FUNCTION','CLR_TABLE_VALUED_FUNCTION','SQL_INLINE_TABLE_VALUED_FUNCTION',
'SQL_STORED_PROCEDURE','CLR_STORED_PROCEDURE','SQL_TABLE_VALUED_FUNCTION','VIEW')
OPEN sp_complete_cursor_level1
FETCH NEXT FROM sp_complete_cursor_level1 INTO @schema, @tbl
WHILE @@FETCH_STATUS = 0
BEGIN
SET @sqlstrall ='SELECT re.referenced_schema_name, re.referenced_entity_name, ao.type_desc, ao.object_id
    FROM ['+@database+'].sys.dm_sql_referenced_entities (''['+@schema+'].['+@tbl+']'',''OBJECT'') re
JOIN ['+@database+'].sys.all_objects ao
ON schema_name (ao.schema_id) =re.referenced_schema_name AND ao.name=re.referenced_entity_name
WHERE re.referenced_minor_id=0
ORDER BY re.referenced_schema_name, re.referenced_entity_name'
insert into #object_maintenance_level1(name_schema,name_object,name_desc,obj_id)
exec (@sqlstrall)
FETCH NEXT FROM sp_complete_cursor_level1 INTO @schema, @tbl
END
CLOSE sp_complete_cursor_level1
DEALLOCATE sp_complete_cursor_level1
--add object procedure 1 level
insert into #object_maintenance_start (name_schema,name_object,name_desc,obj_id)
select name_schema,name_object,name_desc,obj_id from #object_maintenance_level1
--select schema, table, stat
IF OBJECT_ID('tempdb..#object_maintenance_end') IS NOT NULL DROP TABLE #object_maintenance_end;
create table #object_maintenance_end
(id int identity(1,1) primary key,
name_schema sysname,
name_object sysname,
name_stat sysname
);

DECLARE @table_in1 nvarchar(100) = '#object_maintenance_start'
set @sqlstrall  = N'select oms.name_schema,oms.name_object,  stat.name
	from '+@table_in1+' oms
	JOIN ['+@database+'].sys.stats AS stat   
	ON stat.object_id=oms.obj_id
	CROSS APPLY ['+@database+'].sys.dm_db_stats_properties(stat.object_id, stat.stats_id) AS sp  
	where oms.name_desc in (''USER_TABLE'',''VIEW'')
	order by stat.name';
insert into #object_maintenance_end (name_schema, name_object, name_stat)
exec (@sqlstrall)
--create script update statistics
DECLARE sp_complete_cursor_end  CURSOR FAST_FORWARD FOR 
select name_schema, name_object, name_stat from #object_maintenance_end 
OPEN sp_complete_cursor_end
FETCH NEXT FROM sp_complete_cursor_end INTO @schema, @tbl, @stat
WHILE @@FETCH_STATUS = 0
BEGIN
if @fullscan=0 SET @sqlstrall ='use ['+@database+']; UPDATE STATISTICS ['+@schema+']'+'.['+@tbl+']'+' (['+ @stat+']) with maxdop='+cast (@maxdop as nvarchar(6))+';'
if @fullscan=1 SET @sqlstrall ='use ['+@database+']; UPDATE STATISTICS ['+@schema+']'+'.['+@tbl+']'+' (['+ @stat+']) with fullscan, maxdop='+cast (@maxdop as nvarchar(6))+';'
print @sqlstrall
FETCH NEXT FROM sp_complete_cursor_end INTO @schema, @tbl, @stat
END
CLOSE sp_complete_cursor_end
DEALLOCATE sp_complete_cursor_end

IF OBJECT_ID('tempdb..#object_maintenance_start') IS NOT NULL DROP TABLE #object_maintenance_start;
IF OBJECT_ID('tempdb..#object_maintenance_level1') IS NOT NULL DROP TABLE #object_maintenance_level1;
IF OBJECT_ID('tempdb..#object_maintenance_end') IS NOT NULL DROP TABLE #object_maintenance_end;

end




