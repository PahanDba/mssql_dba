IF OBJECT_ID('dbo.quick_update_stats_proc_sb') IS NULL
  EXEC ('CREATE PROCEDURE dbo.quick_update_stats_proc_sb AS RETURN 0;');
GO
ALTER PROCEDURE [dbo].[quick_update_stats_proc_sb]
( 
	@database sysname,
	@sql_object sysname,
	@fullscan tinyint=0,
	@maxdop int =0,
	@sb tinyint=0,
	@database_out sysname = null,
	@sch_out sysname =null,
	@tbl_out sysname =null
)
/*============================================================================
  File:     quick_update_stats_proc_sb.sql
  
  Summary:  Generate script for update statistics on SQL Server.
  
  SQL Server Versions: 2014 onwards
------------------------------------------------------------------------------
 Written by Pavel A. Polikov https://github.com/PahanDba/mssql_dba

Create 2022-12-18
Fix 2022-12-19: Changed the name of the procedure in the example.

Purpose: updating the statistics of objects used in the procedure.
Objects that are used in the 1st level procedures in the source procedure are taken into account.
Example, procedure prots1 is started. Inside the procedure, there is work with different tables: table1, table2, table3,
and procedure prots2 is called. Inside the proc2 procedure, work is being done with the tables table4, table5, table6,
and the procedure proc3 is called. Inside the proc3 procedure, work is being done with the tables table7, table8, table9.
Will generate an update of each statistics for tables table1, table2, table3, table4, table5, table6.
proc3 procedure objects are not processed.
The procedure does not take into account incremental statistics and partitioning.
Operation modes of the procedure:
1. A script is created to update the statistics, which must be executed manually in SSMS.
Launch example
exec master.dbo.quick_update_stats_proc_sb @database='sqlnexus', @sql_object='[dbo].[test1]' ,@fullscan=0, @maxdop=1
@database - the name of the database where the procedure is located
@sql_object - the schema and name of the procedure whose object statistics are to be updated
@fullscan - use fullscan option when updating statistics
@maxdop - number of threads to update this statistic
2. A list of statistics is created in the table and the statistics are updated using the service broker.
Launch example
exec master.dbo.quick_update_stats_proc_sb @database='sqlnexus', @sql_object='[dbo].[test1]' ,@fullscan=0, @maxdop=1, @sb=1, @database_out='sbdbname', @sch_out ='dbo', @tbl_out='logout'
@database - the name of the database containing the procedure whose objects are to be updated
@sql_object - the schema and name of the procedure whose object statistics are to be updated
@fullscan - use fullscan option when updating statistics
@maxdop - number of threads to update this statistic
@sb - use a broker service so that the update is performed automatically
@database_out - the database in which the objects for the service broker will be created
@sch_out - the scheme in which the objects for the service broker will be created
@tbl_out - service broker tables and objects for the service broker to work with

Conditions for updating statistics through the broker service:
1. The @database owner must be sa
2. Database owner @database_out must be sa
3. The trustworthy parameter for the @database_out database must be ON
4. The is_broker_enabled parameter for the @database_out database must be true
5. Trace flag 7471 must be enabled globally.

Name of objects for work control:
1. @database_out.@sch_out.@tbl_out - a table where all the statistics that need to be updated are placed. Records are removed from it as the service broker updates statistics
2. @database_out.@sch_out.@tbl_out_print - table where records about processed statistics are placed: schema - table - statistics - update start time - update end time
3. Naming message type @msgtype_out='USMsgT_'+@tbl_out
4. Naming contract @contract_out='USContract_'+@tbl_out
5. Naming the queue @queue_out='USQueue_'+@tbl_out
6. Naming service @service_out='USService_'+@tbl_out

To control the operation of the service broker, when updating statistics after starting the procedure via print, the following will be displayed:
1. Scripts for viewing what needs to be processed, what is processed, the list of service broker queues, managing the number of service broker threads.
2. Scripts for deleting objects that were created by the procedure, excluding the table with the results of the work.

Notes:
1. It is better to place the dbo.quick_update_stats_sb procedure in the master database.
2. The procedure for sending messages to the broker service @sb_sendstat = 'dbo.sb_sendstat_'+@tbl_out is placed in the @database_out database.
3. The procedure for receiving messages from the broker service @sb_recievstat = 'dbo.sb_recievstat_'+@tbl_out is placed in the @database_out database.



 Example: 
 exec master.dbo.quick_update_stats_proc_sb @database='sqlnexus', @sql_object='[dbo].[test1]' ,@fullscan=0, @maxdop=1, @sb=1, @database_out='sbdbname', @sch_out='dbo', @tbl_out='logout'
 exec master.dbo.quick_update_stats_proc_sb @database='sqlnexus', @sql_object='[dbo].[test1]' ,@fullscan=1, @maxdop=1, @sb=1, @database_out='sbdbname', @sch_out='dbo', @tbl_out='logout'
 exec master.dbo.quick_update_stats_proc_sb @database='sqlnexus', @sql_object='[dbo].[test1]' ,@fullscan=0, @maxdop=1

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
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
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
		@ErrorState INT,
		@table_in1 nvarchar(100) = '#object_maintenance_start',
		@table_in2 nvarchar(100) = '#object_maintenance_end',
		@ParmDefinition NVARCHAR(1000),
		@is_broker_enabled bit,
		@owner_database nvarchar(128), --owner database update statistics
		@owner_database_out sysname, ----owner database for service broker
		@status_trustworthy_database int, --status trustworthy database update statistics
		@status_trustworthy_database_out int, --status trustworthy database for service broker
		@msgtype_out nvarchar (140), --create message type for service broker database
		@msgtype_out_check nvarchar (140), --check exists message type for service broker database
		@contract_out nvarchar (140), --create contract for service broker database
		@contract_out_check nvarchar (140), --check exists contract for service broker database
		@queue_out nvarchar (140), --create queue for service broker database
		@queue_out_check nvarchar (140), --check exists queue for service broker database
		@service_out nvarchar (140), --create service for service broker database
		@service_out_check nvarchar (140), --check exists service for service broker database
		@tbl_out_print sysname, --log running udate statistics
		@status_tf tinyint , --Indicates whether the trace flag is set ON of OFF, either globally or for the session.
		@global_tf tinyint, --Indicates whether the trace flag is set globally
		@sb_sendstat sysname, --Procedure for sending messages service broker
		@sb_recievstat sysname --Procedure for receiving messages service broker
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
if @sb>=1 set @sb=1
set @tbl_out_print=@tbl_out+'_print'
IF OBJECT_ID('tempdb..#object_maintenance_start') IS NOT NULL DROP TABLE #object_maintenance_start;
create table #object_maintenance_start
(id int identity(1,1) primary key,
name_schema sysname,
name_object sysname,
name_desc sysname,
obj_id bigint,
database_work sysname,
maxdop_work tinyint,
sb_work tinyint,
fullscan_work tinyint
);
set @sqlstrall='SELECT re.referenced_schema_name, re.referenced_entity_name, ao.type_desc, ao.object_id,'''+@database+''','+cast (@maxdop as nvarchar(2))+','+cast (@sb as nvarchar(2))+','+cast(@fullscan as nvarchar(2))+'
    FROM ['+@database+'].sys.dm_sql_referenced_entities ('''+@sql_object+''',''OBJECT'') re
JOIN ['+@database+'].sys.all_objects ao
ON schema_name (ao.schema_id) =re.referenced_schema_name AND ao.name=re.referenced_entity_name
WHERE re.referenced_minor_id=0
ORDER BY re.referenced_schema_name, re.referenced_entity_name'
insert into #object_maintenance_start(name_schema,name_object,name_desc,obj_id,database_work,maxdop_work,sb_work,fullscan_work  )
exec ( @sqlstrall)
IF OBJECT_ID('tempdb..#object_maintenance_level1') IS NOT NULL DROP TABLE #object_maintenance_level1;
create table #object_maintenance_level1
(id int identity(1,1) primary key,
name_schema sysname,
name_object sysname,
name_desc sysname,
obj_id bigint,
database_work sysname,
maxdop_work tinyint,
sb_work tinyint,
fullscan_work tinyint
);
DECLARE sp_complete_cursor_level1  CURSOR FAST_FORWARD FOR 
select name_schema,name_object from #object_maintenance_start 
where name_desc in ('SQL_SCALAR_FUNCTION','CLR_SCALAR_FUNCTION','CLR_TABLE_VALUED_FUNCTION','SQL_INLINE_TABLE_VALUED_FUNCTION',
'SQL_STORED_PROCEDURE','CLR_STORED_PROCEDURE','SQL_TABLE_VALUED_FUNCTION','VIEW')
OPEN sp_complete_cursor_level1
FETCH NEXT FROM sp_complete_cursor_level1 INTO @schema, @tbl
WHILE @@FETCH_STATUS = 0
BEGIN
SET @sqlstrall ='SELECT re.referenced_schema_name, re.referenced_entity_name, ao.type_desc, ao.object_id,'''+@database+''','+cast (@maxdop as nvarchar(2))+','+cast (@sb as nvarchar(2))+','+cast(@fullscan as nvarchar(2))+'
    FROM ['+@database+'].sys.dm_sql_referenced_entities (''['+@schema+'].['+@tbl+']'',''OBJECT'') re
JOIN ['+@database+'].sys.all_objects ao
ON schema_name (ao.schema_id) =re.referenced_schema_name AND ao.name=re.referenced_entity_name
WHERE re.referenced_minor_id=0
ORDER BY re.referenced_schema_name, re.referenced_entity_name'
insert into #object_maintenance_level1(name_schema,name_object,name_desc,obj_id,database_work,maxdop_work,sb_work,fullscan_work)
exec (@sqlstrall)
FETCH NEXT FROM sp_complete_cursor_level1 INTO @schema, @tbl
END
CLOSE sp_complete_cursor_level1
DEALLOCATE sp_complete_cursor_level1
--add object procedure 1 level
insert into #object_maintenance_start (name_schema,name_object,name_desc,obj_id,database_work,maxdop_work,sb_work,fullscan_work)
select name_schema,name_object,name_desc,obj_id ,database_work,maxdop_work,sb_work,fullscan_work from #object_maintenance_level1
--select schema, table, stat
IF OBJECT_ID('tempdb..#object_maintenance_end') IS NOT NULL DROP TABLE #object_maintenance_end;
create table #object_maintenance_end
(id int identity(1,1) primary key,
name_schema sysname,
name_object sysname,
name_stat sysname,
database_work sysname,
maxdop_work tinyint,
sb_work tinyint,
fullscan_work tinyint
);
set @sqlstrall  = N'select oms.name_schema,oms.name_object,  stat.name,'''+@database+''','+cast (@maxdop as nvarchar(2))+','+cast (@sb as nvarchar(2))+','+cast(@fullscan as nvarchar(2))+'
	from '+@table_in1+' oms
	JOIN ['+@database+'].sys.stats AS stat   
	ON stat.object_id=oms.obj_id
	CROSS APPLY ['+@database+'].sys.dm_db_stats_properties(stat.object_id, stat.stats_id) AS sp  
	where oms.name_desc in (''USER_TABLE'',''VIEW'')
	order by stat.name';
insert into #object_maintenance_end (name_schema, name_object, name_stat,database_work,maxdop_work,sb_work,fullscan_work)
exec (@sqlstrall)
if @sb=0
	begin
		--create script update statistics
		DECLARE sp_complete_cursor_end  CURSOR FAST_FORWARD FOR 
		select name_schema, name_object, name_stat from #object_maintenance_end group by name_schema, name_object, name_stat order by name_stat
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
	end
if @sb=1
	begin
		if 	@database_out is null or @sch_out is null or @tbl_out is null
				begin
					set @ErrorMessage ='One of the options @database_out or @sch_out or @tbl_out  NULL'
						RAISERROR (@ErrorMessage, -- Message text.  
								   16, -- Severity.  
								   1 -- State.  
								   );  
					return 1
				end

		else
			begin
				--checking status ang global trace flag 7471 
				--https://support.microsoft.com/en-us/topic/kb3156157-running-multiple-update-statistics-for-different-statistics-on-a-single-table-concurrently-is-available-300f174d-c34d-6635-42f6-db58497cd281
				--or
				--https://learn.microsoft.com/en-us/archive/blogs/sql_server_team/boosting-update-statistics-performance-with-sql-2014-sp1cu6
				set @sqlstrall= N'
					IF OBJECT_ID(''tempdb..#check_tf'') IS NOT NULL DROP TABLE #check_tf;
					create table #check_tf (traceflag nvarchar(10) null, status_tf tinyint null, global_tf tinyint null, session_tf  tinyint null);
					insert into #check_tf (traceflag, status_tf, global_tf, session_tf)
					exec(''dbcc tracestatus()'');
					if exists (select top 1 * from #check_tf where traceflag=''7471'')
					begin 
					select @status_tf=(isnull(status_tf,0)) , @global_tf=(isnull(global_tf,0)) from #check_tf where traceflag=''7471''
					end
					else
					begin
					set @status_tf=0
					set @global_tf=0
					end
					IF OBJECT_ID(''tempdb..#check_tf'') IS NOT NULL DROP TABLE #check_tf;'
				set @ParmDefinition =N'@status_tf tinyint OUTPUT, @global_tf tinyint OUTPUT'
				execute sp_executesql  @sqlstrall, @ParmDefinition, @status_tf=@status_tf OUTPUT, @global_tf=@global_tf OUTPUT;
				if @status_tf!=1 or @global_tf!=1
				begin
					set @ErrorMessage ='Trace flag 7471 not enabled globally. Description of the trace flag https://learn.microsoft.com/en-us/archive/blogs/sql_server_team/boosting-update-statistics-performance-with-sql-2014-sp1cu6 . Run script for enabled  Trace flag 7471: use master; dbcc traceon (7471,-1);  '
						RAISERROR (@ErrorMessage, -- Message text.  
								   16, -- Severity.  
								   1 -- State.  
								   );  
					return 1
				end

				--checking database owner to update statistics
				set @sqlstrall= N'SELECT @owner_database=suser_sname( owner_sid ) FROM master.sys.databases where name='''+@database+''''
				set @ParmDefinition =N'@database sysname, @owner_database sysname OUTPUT'
				execute sp_executesql  @sqlstrall, @ParmDefinition, @database, @owner_database=@owner_database OUTPUT;
				if @owner_database!='sa' 
				begin
					set @ErrorMessage ='The database owner ['+@database+'] is not sa. Run script for change dbowner: use master; exec ['+@database+'].dbo.sp_changedbowner''sa''; '
						RAISERROR (@ErrorMessage, -- Message text.  
								   16, -- Severity.  
								   1 -- State.  
								   );  
					return 1
				end
				--checking database owner to run service broker
				set @sqlstrall= N'SELECT @owner_database_out=suser_sname( owner_sid ) FROM master.sys.databases where name='''+@database_out+''''
				set @ParmDefinition =N'@database_out sysname, @owner_database_out sysname OUTPUT'
				execute sp_executesql  @sqlstrall, @ParmDefinition, @database_out, @owner_database_out=@owner_database_out OUTPUT;
				if @owner_database_out!='sa' 
				begin
					set @ErrorMessage ='The database owner ['+@database_out+'] is not sa. Run script for change dbowner: use master; exec ['+@database_out+'].dbo.sp_changedbowner''sa''; '
						RAISERROR (@ErrorMessage, -- Message text.  
								   16, -- Severity.  
								   1 -- State.  
								   );  
					return 1
				end
				--checking trustworthy to service broker
				set @sqlstrall= N'SELECT @status_trustworthy_database_out=is_trustworthy_on FROM master.sys.databases where name='''+@database_out+''''
				set @ParmDefinition =N'@database_out sysname, @status_trustworthy_database_out int OUTPUT'
				execute sp_executesql  @sqlstrall, @ParmDefinition, @database_out, @status_trustworthy_database_out=@status_trustworthy_database_out OUTPUT;
				if @status_trustworthy_database_out!=1
				begin
					set @ErrorMessage ='The status of trustworthy in database ['+@database_out+'] is OFF. Run script for change trustworthy: use master; alter database ['+@database_out+'] set trustworthy on;'
						RAISERROR (@ErrorMessage, -- Message text.  
								   16, -- Severity.  
								   1 -- State.  
								   );  
					return 1
				end
				--get status service broker
				set @sqlstrall=N'select @is_broker_enabled1=(SELECT is_broker_enabled FROM sys.databases
				WHERE name = '''+@database_out+''')'
				set @ParmDefinition =N'@is_broker_enabled1 bit OUTPUT'
				execute sp_executesql  @sqlstrall, @ParmDefinition, @is_broker_enabled1=@is_broker_enabled OUTPUT;
				--enabled service broker if false
				if @is_broker_enabled='false' 
				begin
					set @ErrorMessage ='The service broker on database ['+@database_out+'] is Disable. Run script for enable service broker: use master; alter database ['+@database_out+'] SET ENABLE_BROKER WITH ROLLBACK IMMEDIATE;'
						RAISERROR (@ErrorMessage, -- Message text.  
								   16, -- Severity.  
								   1 -- State.  
								   );  
					return 1
				end
				--create output table include all statistics for service broker
				SET @sqlstrall='
				if  exists (select * from ['+@database_out+'].sys.objects where name ='''+@tbl_out+''' and schema_id =(select schema_id from ['+@database_out+'].sys.schemas where name ='''+@sch_out+''') ) DROP TABLE ['+@database_out+'].['+@sch_out+'].['+@tbl_out+'];
				begin
				USE ['+@database_out+'];
				SET ansi_nulls on;
				SET quoted_identifier on;
				CREATE TABLE ['+@sch_out+'].['+@tbl_out+'](
					[id] int  IDENTITY (1,1) NOT NULL
					  , CONSTRAINT PK_'+@sch_out+''+@tbl_out+'_id PRIMARY KEY CLUSTERED (id), 
					name_schema sysname,
					name_object sysname,
					name_stat sysname,
					database_work sysname,
					maxdop_work tinyint,
					sb_work tinyint,
					fullscan_work tinyint)
					on [primary];
				end	'
				EXEC (@sqlstrall)
				--insert all statistics in output table
				set @sqlstrall='insert into ['+@database_out+'].['+@sch_out+'].['+@tbl_out+'] (name_schema, name_object, name_stat, database_work, maxdop_work, sb_work, fullscan_work)
				select name_schema, name_object, name_stat,'''+@database+''','+cast (@maxdop as nvarchar(2))+','+cast (@sb as nvarchar(2))+','+cast(@fullscan as nvarchar(2))+' from '+@table_in2+' group by name_schema, name_object, name_stat order by name_stat '
				EXEC (@sqlstrall)
				--check message type on database for service broker 
				set @msgtype_out='USMsgT_'+@tbl_out
				set @sqlstrall=N'USE ['+@database_out+'];
				select @msgtype_out_check=(select name from sys.service_message_types where name='''+@msgtype_out+''')'
				set @ParmDefinition =N'@msgtype_out nvarchar (140), @msgtype_out_check nvarchar (140) OUTPUT'
				execute sp_executesql  @sqlstrall, @ParmDefinition, @msgtype_out, @msgtype_out_check=@msgtype_out_check OUTPUT;
				if @msgtype_out_check=@msgtype_out
				begin
					set @ErrorMessage ='The message type ['+@msgtype_out+'] service broker on database ['+@database_out+'] is exists. You need to remove the message type ['+@msgtype_out+'] or choose a different name for the parameter @tbl_out.'
						RAISERROR (@ErrorMessage, -- Message text.  
								   16, -- Severity.  
								   1 -- State.  
								   );  
					return 1
				end
				--create message type on database for service broker 
				set @sqlstrall='USE ['+@database_out+'];
				CREATE MESSAGE TYPE '+@msgtype_out+'
				AUTHORIZATION dbo
				VALIDATION = None;'
				EXEC (@sqlstrall)
				--check contract on database for service broker
				set @contract_out='USContract_'+@tbl_out
				set @sqlstrall=N'USE ['+@database_out+'];
				select @contract_out_check=(select name from sys.service_contracts where name='''+@contract_out+''')'
				set @ParmDefinition =N'@contract_out nvarchar (140), @contract_out_check nvarchar (140) OUTPUT'
				execute sp_executesql  @sqlstrall, @ParmDefinition, @contract_out, @contract_out_check=@contract_out_check OUTPUT;
				if @contract_out_check=@contract_out
				begin
					set @ErrorMessage ='The contract ['+@contract_out+'] service broker on database ['+@database_out+'] is exists. You need to remove the contract ['+@contract_out+'] or choose a different name for the parameter @tbl_out.'
						RAISERROR (@ErrorMessage, -- Message text.  
								   16, -- Severity.  
								   1 -- State.  
								   );  
					return 1
				end
				--create contract on database for service broker 
				set @sqlstrall='USE ['+@database_out+'];
				CREATE CONTRACT '+@contract_out+'
				('+@msgtype_out+' SENT BY ANY)'
				EXEC (@sqlstrall)
				--check queue on database for service broker
				set @queue_out='USQueue_'+@tbl_out
				set @sqlstrall=N'USE ['+@database_out+'];
				select @queue_out_check=(select name from sys.service_contracts where name='''+@queue_out+''')'
				set @ParmDefinition =N'@queue_out nvarchar (140), @queue_out_check nvarchar (140) OUTPUT'
				execute sp_executesql  @sqlstrall, @ParmDefinition, @queue_out, @queue_out_check=@queue_out_check OUTPUT;
				if @queue_out_check=@queue_out
				begin
					set @ErrorMessage ='The queue ['+@queue_out+'] service broker on database ['+@database_out+'] is exists. You need to remove the queue ['+@queue_out+'] or choose a different name for the parameter @tbl_out.'
						RAISERROR (@ErrorMessage, -- Message text.  
								   16, -- Severity.  
								   1 -- State.  
								   );  
					return 1
				end
				--create queue on database for service broker 
				set @sqlstrall='USE ['+@database_out+'];
				CREATE QUEUE '+@queue_out+'
				WITH STATUS = ON, RETENTION = OFF'
				EXEC (@sqlstrall)
				--check service on database for service broker
				set @service_out='USService_'+@tbl_out
				set @sqlstrall=N'USE ['+@database_out+'];
				select @service_out_check=(select name from sys.services where name='''+@service_out+''')'
				set @ParmDefinition =N'@service_out nvarchar (140), @service_out_check nvarchar (140) OUTPUT'
				execute sp_executesql  @sqlstrall, @ParmDefinition, @service_out, @service_out_check=@service_out_check OUTPUT;
				if @service_out_check=@service_out
				begin
					set @ErrorMessage ='The service ['+@service_out+'] service broker on database ['+@database_out+'] is exists. You need to remove the service ['+@contract_out+'] or choose a different name for the parameter @tbl_out.'
						RAISERROR (@ErrorMessage, -- Message text.  
								   16, -- Severity.  
								   1 -- State.  
								   );  
					return 1
				end
				--create service on database for service broker 
				set @sqlstrall='USE ['+@database_out+'];
				CREATE SERVICE '+@service_out+'
				AUTHORIZATION dbo 
				ON QUEUE '+@queue_out+' ('+@contract_out+');'
				EXEC (@sqlstrall)
				--create procedure send message for service broker on database for service broker 
				set @sb_sendstat = 'sb_sendstat_'+@tbl_out
				set @sqlstrall='exec '+ QUOTENAME(@database_out) + '..sp_executesql N''
				IF OBJECT_ID(''''dbo.'+@sb_sendstat+''''') IS NULL
				EXEC (''''CREATE PROCEDURE dbo.'+@sb_sendstat+' AS RETURN 0;'''')'''
				EXEC (@sqlstrall)
				set @sqlstrall='exec '+ QUOTENAME(@database_out) + '..sp_executesql N''alter PROCEDURE dbo.'+@sb_sendstat+'
				as
				DECLARE @ch uniqueidentifier = NEWID()
				DECLARE @msg XML
				declare @sqlstrall nvarchar(4000),
						@id int,
						@name_schema sysname,
						@name_object sysname,
						@name_stat sysname,
						@database sysname,
						@maxdop tinyint,
						@sb tinyint,
						@fullscan tinyint
				DECLARE sp_complete_cursor  CURSOR FAST_FORWARD FOR 
				select id, name_schema, name_object, name_stat, database_work, maxdop_work, sb_work, fullscan_work from ['+@database_out+'].['+@sch_out+'].['+@tbl_out+']
				OPEN sp_complete_cursor
				FETCH NEXT FROM sp_complete_cursor INTO @id, @name_schema, @name_object, @name_stat, @database, @maxdop, @sb, @fullscan
				WHILE @@FETCH_STATUS = 0
				BEGIN
				BEGIN DIALOG CONVERSATION @ch
				FROM SERVICE ['+@service_out+']
				TO SERVICE '''''+@service_out+'''''
				ON CONTRACT ['+@contract_out+']
				WITH ENCRYPTION = OFF; -- more possible options
				SET @msg = ( select id=@id, name_schema=@name_schema, name_object=@name_object, name_stat=@name_stat, database_work=@database, maxdop_work=@maxdop, sb_work=@sb, fullscan_work=@fullscan from ['+@database_out+'].['+@sch_out+'].['+@tbl_out+']
				FOR XML PATH(''''Stat'''')
						,TYPE
				);
				SEND ON CONVERSATION @ch MESSAGE TYPE ['+@msgtype_out+'] (@msg);
				END CONVERSATION @ch;
				FETCH NEXT FROM sp_complete_cursor INTO @id, @name_schema, @name_object, @name_stat, @database, @maxdop, @sb, @fullscan
				END
				CLOSE sp_complete_cursor
				DEALLOCATE sp_complete_cursor'''
				EXEC (@sqlstrall)
				--create output log table include all statistics for service broker
				SET @sqlstrall='
				if  exists (select * from ['+@database_out+'].sys.objects where name ='''+@tbl_out_print+''' and schema_id =(select schema_id from ['+@database_out+'].sys.schemas where name ='''+@sch_out+''') ) DROP TABLE ['+@database_out+'].['+@sch_out+'].['+@tbl_out_print+'];
				begin
				USE ['+@database_out+'];
				SET ansi_nulls on;
				SET quoted_identifier on;
				CREATE TABLE ['+@sch_out+'].['+@tbl_out_print+'](
					[id] int  IDENTITY (1,1) NOT NULL
					  , CONSTRAINT PK_'+@sch_out+''+@tbl_out_print+'_id PRIMARY KEY CLUSTERED (id), 
					name_schema sysname,
					name_object sysname,
					name_stat sysname,
					begin_time datetime null,
					end_time datetime null)
					on [primary];
				end	'
				EXEC (@sqlstrall)
				--create procedure reciev message for service broker on database for service broker
				set @sb_recievstat='sb_recievstat_'+@tbl_out
				set @sqlstrall='exec '+ QUOTENAME(@database_out) + '..sp_executesql N''
				IF OBJECT_ID(''''dbo.'+@sb_recievstat+''''') IS NULL
				EXEC (''''CREATE PROCEDURE dbo.'+@sb_recievstat+' AS RETURN 0;'''')'''
				--print @sqlstrall
				EXEC (@sqlstrall)
				set @sqlstrall='exec '+ QUOTENAME(@database_out) + '..sp_executesql N''ALTER PROCEDURE [dbo].['+@sb_recievstat+'] 
				AS
				BEGIN
				declare
				@message_body VARBINARY(MAX),
				@message_body1 xml,
				@message_type_name nvarchar(256),
				@conversation_handle uniqueidentifier,
				@messagetypename nvarchar(256),
				@ret_status INT=0,
				@ParmDefinition nVARCHAR(300)=N''''@schema sysname, @table sysname, @stats sysname, @maxdop tinyint, @fullscan tinyint, @ret_status INT OUTPUT''''
				declare 
				@table sysname,
				@stats sysname,
				@schema sysname,
				@database sysname, 
				@maxdop tinyint, 
				@sb tinyint, 
				@fullscan tinyint,
				@iden_sc int,
				@id int,
				@sqlstrall nvarchar(4000)
				declare @xmlBody XML
				WAITFOR(
				RECEIVE TOP(1)
				@message_body = message_body,
				@message_type_name = message_type_name,
				@conversation_handle = conversation_handle,
				@messagetypename = message_type_name
				FROM dbo.'+@queue_out+'
				), TIMEOUT 1000;
				IF (@messagetypename = '''''+@msgtype_out+''''')
				BEGIN
						select @message_body1 = CAST(@message_body as xml)
						SELECT @id=@message_body1.value(''''(//Stat/id)[1]'''', ''''int''''),
							@schema=@message_body1.value(''''(//Stat/name_schema)[1]'''', ''''sysname''''),
							@table=@message_body1.value(''''(//Stat/name_object)[1]'''', ''''sysname''''),
							@stats=@message_body1.value(''''(//Stat/name_stat)[1]'''', ''''sysname''''),
							@maxdop=@message_body1.value(''''(//Stat/maxdop_work)[1]'''', ''''tinyint''''),
							@fullscan=@message_body1.value(''''(//Stat/fullscan_work)[1]'''', ''''tinyint'''');
						insert into ['+@database_out+'].['+@sch_out+'].['+@tbl_out_print+'] ([name_schema],[name_object], [name_stat], begin_time)
						select @schema,@table,@stats,getdate()
						select @iden_sc=SCOPE_IDENTITY();
						delete from ['+@database_out+'].['+@sch_out+'].['+@tbl_out+']
						where id=@id and [name_schema]=@schema  and [name_object]=@table and [name_stat]=@stats
						if @fullscan=0 SET @sqlstrall =''''use ['+@database+']; UPDATE STATISTICS [''''+@schema+'''']'+'.[''''+@table+'''']'+' ([''''+ @stats+'''']) with maxdop=''''+cast (@maxdop as nvarchar(6))+'''';''''
						if @fullscan=1 SET @sqlstrall =''''use ['+@database+']; UPDATE STATISTICS [''''+@schema+'''']'+'.[''''+@table+'''']'+' ([''''+ @stats+'''']) with fullscan, maxdop=''''+cast (@maxdop as nvarchar(6))+'''';''''

						  EXEC sp_ExecuteSQL @sqlstrall, @ParmDefinition, @schema=@schema, @table=@table, @stats=@stats, @maxdop=@maxdop, @fullscan=@fullscan, @ret_status=@ret_status OUTPUT
						  if @ret_status=0
						  begin
						  update ['+@database_out+'].['+@sch_out+'].['+@tbl_out_print+']
						  set end_time=getdate()
						  where id=@iden_sc
						  end
				END
				IF (@messagetypename = ''''http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog'''')
				BEGIN
				END CONVERSATION @conversation_handle;
				END
				END'''
				EXEC (@sqlstrall)
				--Changing the number of service broker threads.
				set @sqlstrall='exec '+ QUOTENAME(@database_out) + '..sp_executesql N''ALTER QUEUE '+@queue_out+'
				WITH ACTIVATION 
				(
				   STATUS = on,
				   PROCEDURE_NAME = dbo.'+@sb_recievstat+',
				   MAX_QUEUE_READERS = 4,
				   EXECUTE AS OWNER
				)
				'''
				EXEC (@sqlstrall)
				--start procedure send message for service broker on database for service broker 
				set @sqlstrall='exec '+ QUOTENAME(@database_out) + '..sp_executesql N''exec dbo.'+@sb_sendstat+'
				'''
				EXEC (@sqlstrall)
				--scripts for monitoring and drop objects after end update statistics
				print '--Script to get a list of statistics to be updated.'
				set @sqlstrall='SELECT [id], [name_schema], [name_object], [name_stat], [database_work], [maxdop_work], [sb_work], [fullscan_work]
					FROM ['+@database_out+'].['+@sch_out+'].['+@tbl_out+']'
				print @sqlstrall
				print '--================================================='
				print '--Script to get a list of statistics that have already been updated.'
				set @sqlstrall='SELECT [id], [name_schema], [name_object], [name_stat], [begin_time], [end_time]
					FROM ['+@database_out+'].['+@sch_out+'].['+@tbl_out_print+']'
				print @sqlstrall
				print '--================================================='
				print '--Script to change the number of service broker threads.'
				print '/*================================================='
				set @sqlstrall='USE ['+@database_out+'];
				ALTER QUEUE '+@queue_out+'
				WITH ACTIVATION 
				(
				   STATUS = on,
				   PROCEDURE_NAME = dbo.'+@sb_recievstat+',
				   MAX_QUEUE_READERS = 4, --Change 4 to whatever number you need 
				   EXECUTE AS OWNER
				)'
				print @sqlstrall
				print '=================================================*/'
				print '--Script to get a list of messages in the service broker queue.'
				set @sqlstrall='select *, casted_message_body = 
					CASE message_type_name WHEN ''X''
					  THEN CAST(message_body AS NVARCHAR(MAX)) 
					  ELSE message_body 
					END  from ['+@database_out+'].[dbo].['+@queue_out+']'
				print @sqlstrall
				print '/*================================================='
				print 'Attention! The scripts below delete all objects that were created for the service broker to work.'
				set @sqlstrall='USE ['+@database_out+']
					GO
					DROP SERVICE ['+@service_out+']
					GO
					DROP QUEUE [dbo].['+@queue_out+']
					GO
					DROP CONTRACT ['+@contract_out+']
					GO
					DROP MESSAGE TYPE ['+@msgtype_out+']
					GO
					DROP TABLE ['+@database_out+'].['+@sch_out+'].['+@tbl_out_print+']
					GO
					DROP TABLE ['+@database_out+'].['+@sch_out+'].['+@tbl_out+']
					go
					DROP PROCEDURE dbo.['+@sb_sendstat+']
					go
					DROP PROCEDURE dbo.['+@sb_recievstat+']
					go'
				print @sqlstrall
				print '=================================================*/'
			end
	end
IF OBJECT_ID('tempdb..#object_maintenance_start') IS NOT NULL DROP TABLE #object_maintenance_start;
IF OBJECT_ID('tempdb..#object_maintenance_level1') IS NOT NULL DROP TABLE #object_maintenance_level1;
IF OBJECT_ID('tempdb..#object_maintenance_end') IS NOT NULL DROP TABLE #object_maintenance_end;
end




