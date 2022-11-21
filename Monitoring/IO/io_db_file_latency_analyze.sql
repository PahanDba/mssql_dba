USE [master]
GO

/****** Object:  StoredProcedure [dbo].[ram_distribution_analyze]    Script Date: 18.11.2022 13:41:32 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO
/*============================================================================
  File:     io_db_file_latency_analyze.sql
  
  Summary:  Analyze Data collection io latency each database each file data on SQL Server.
  
  SQL Server Versions: 2014 onwards
------------------------------------------------------------------------------
 Written by Pavel A. Polikov https://github.com/PahanDba/mssql_dba
 Last update 17.11.2022
 example 
 exec dbo.io_db_file_latency_analyze @DatabaseAnalyze='msdb',@SchemaAnalyze='dbo', @TableAnalyze='monitor_io_db_file_latency',@time_startAnalyze='2022-11-18 14:05:11', @time_endAnalyze='2022-11-19 14:42:00'

 The original script is taken from
  https://www.sqlskills.com/blogs/paul/capturing-io-latencies-period-time/
 Summary:  Short snapshot of I/O latencies
 
  SQL Server Versions: 2005 onwards
------------------------------------------------------------------------------
  Written by Paul S. Randal, SQLskills.com
 
  (c) 2014, SQLskills.com. All rights reserved.
 
  For more scripts and sample code, check out http://www.SQLskills.com
 
  You may alter this code for your own *non-commercial* purposes (e.g. in a
  for-sale commercial tool). Use in your own environment is encouraged.
  You may republish altered code as long as you include this copyright and
  give due credit, but you must obtain prior permission before blogging
  this code.
 
  THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF
  ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED
  TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
  PARTICULAR PURPOSE.
============================================================================*/
IF OBJECT_ID('dbo.io_db_file_latency_analyze') IS NULL
  EXEC ('CREATE PROCEDURE dbo.io_db_file_latency_analyze AS RETURN 0;');
GO
ALTER procedure [dbo].[io_db_file_latency_analyze]
(
@DatabaseAnalyze NVARCHAR(300),
@SchemaAnalyze NVARCHAR(300),
@TableAnalyze NVARCHAR(300),
@time_startAnalyze datetime,
@time_endAnalyze datetime
)
as

begin

declare @sqlstring nvarchar(max),
		@time_startAnalyze_real datetime,
		@time_endAnalyze_real datetime,
		@ParmDefinition NVARCHAR(500)
set @sqlstring=N'select @time_startAnalyze_real1=(select top 1 collection_date
from ['+@DatabaseAnalyze+'].['+@SchemaAnalyze+'].['+@TableAnalyze+']
where collection_date>='''+cast (@time_startAnalyze as nvarchar(30))+'''
order by collection_date asc)'
set @ParmDefinition =N'@time_startAnalyze_real1 char (19) OUTPUT'
execute sp_executesql  @sqlstring, @ParmDefinition, @time_startAnalyze_real1=@time_startAnalyze_real OUTPUT;

set @sqlstring=N'select @time_endAnalyze_real1=(select top 1 collection_date
from ['+@DatabaseAnalyze+'].['+@SchemaAnalyze+'].['+@TableAnalyze+']
where collection_date<='''+cast (@time_endAnalyze as nvarchar(30))+'''
order by collection_date desc)'
set @ParmDefinition =N'@time_endAnalyze_real1 char (19) OUTPUT'
execute sp_executesql  @sqlstring, @ParmDefinition, @time_endAnalyze_real1=@time_endAnalyze_real OUTPUT;

select 'Requested interval' as 'Comment',@time_startAnalyze as 'Time Start',@time_endAnalyze  as 'Time End', cast (datediff (hh,@time_startAnalyze,@time_endAnalyze) as varchar(4))+' / '+cast(datediff (mi,@time_startAnalyze,@time_endAnalyze) as varchar(100))+' / '+cast(datediff (ss,@time_startAnalyze,@time_endAnalyze) as varchar(100)) as 'Interval HH/MM/SS'
union all
select 'Found time interval'as 'Comment',@time_startAnalyze_real  as 'Time Start',@time_endAnalyze_real  as 'Time End', cast(datediff (hh,@time_startAnalyze_real,@time_endAnalyze_real) as varchar(4))+' / '+cast(datediff (mi,@time_startAnalyze_real,@time_endAnalyze_real) as varchar(100))+' / '+cast(datediff (ss,@time_startAnalyze_real,@time_endAnalyze_real) as varchar(100)) as 'Interval HH/MM/SS'

/*
set @sqlstring='
select '''+convert (char (19),@time_startAnalyze, 121)+''','''+convert (char (19),@time_endAnalyze, 121)+''',
,(select top 1 collection_date
from ['+@DatabaseAnalyze+'].['+@SchemaAnalyze+'].['+@TableAnalyze+']
where collection_date<='''+cast (@time_endAnalyze as nvarchar(30))+'''
order by collection_date desc)
;WITH 
time_startAnalyze_cte as
(select [file_handle], [database_id], [file_id], [DatabaseName], [avg_read_latency_ms], [avg_write_latency_ms]
      ,[avg_io_latency_ms], [File_Size_MB], [physical_name], [type_desc], [io_stall_read_ms], [num_of_reads], [io_stall_write_ms]
      ,[num_of_writes], [io_stalls_ms], [total_io],[num_of_bytes_read],[num_of_bytes_written], [collection_date]
	  FROM ['+@DatabaseAnalyze+'].['+@SchemaAnalyze+'].['+@TableAnalyze+'] 
	  where collection_date=(select top 1 collection_date
from ['+@DatabaseAnalyze+'].['+@SchemaAnalyze+'].['+@TableAnalyze+']
where collection_date>='''+cast (@time_startAnalyze as nvarchar(30))+'''
order by collection_date asc)),
time_endAnalyze_cte as
(select [file_handle], [database_id], [file_id], [DatabaseName], [avg_read_latency_ms], [avg_write_latency_ms]
      ,[avg_io_latency_ms], [File_Size_MB], [physical_name], [type_desc], [io_stall_read_ms], [num_of_reads], [io_stall_write_ms]
      ,[num_of_writes], [io_stalls_ms],[total_io],[num_of_bytes_read],[num_of_bytes_written],[collection_date]
	  FROM ['+@DatabaseAnalyze+'].['+@SchemaAnalyze+'].['+@TableAnalyze+'] 
	  where collection_date=(select top 1 collection_date
from ['+@DatabaseAnalyze+'].['+@SchemaAnalyze+'].['+@TableAnalyze+']
where collection_date<='''+cast (@time_endAnalyze as nvarchar(30))+'''
order by collection_date desc)
),
[DiffLatencies] AS
(SELECT
-- Files that weren''t in the first snapshot
        [ts2].[database_id],
        [ts2].[file_id],
		[ts2].[physical_name],
		[ts2].[type_desc],
        [ts2].[num_of_reads],
        [ts2].[io_stall_read_ms],
        [ts2].[num_of_writes],
        [ts2].[io_stall_write_ms],
        [ts2].[io_stalls_ms],
        [ts2].[num_of_bytes_read],
        [ts2].[num_of_bytes_written]
    FROM time_endAnalyze_cte AS [ts2]
    LEFT OUTER JOIN time_startAnalyze_cte AS [ts1]
        ON [ts2].[file_handle] = [ts1].[file_handle]
    WHERE [ts1].[file_handle] IS NULL
UNION
SELECT
-- Diff of latencies in both snapshots
        [ts2].[database_id],
        [ts2].[file_id],
		[ts2].[physical_name],
		[ts2].[type_desc],
        [ts2].[num_of_reads] - [ts1].[num_of_reads] AS [num_of_reads],
        [ts2].[io_stall_read_ms] - [ts1].[io_stall_read_ms] AS [io_stall_read_ms],
        [ts2].[num_of_writes] - [ts1].[num_of_writes] AS [num_of_writes],
        [ts2].[io_stall_write_ms] - [ts1].[io_stall_write_ms] AS [io_stall_write_ms],
        [ts2].[io_stalls_ms] - [ts1].[io_stalls_ms] AS [io_stall],
        [ts2].[num_of_bytes_read] - [ts1].[num_of_bytes_read] AS [num_of_bytes_read],
        [ts2].[num_of_bytes_written] - [ts1].[num_of_bytes_written] AS [num_of_bytes_written]
    FROM time_endAnalyze_cte AS [ts2]
    LEFT OUTER JOIN time_startAnalyze_cte AS [ts1]
        ON [ts2].[file_handle] = [ts1].[file_handle]
    WHERE [ts1].[file_handle] IS NOT NULL)
SELECT
    DB_NAME (database_id) AS [DB],
    LEFT ([physical_name], 2) AS [Drive],
    [type_desc],
    [num_of_reads] AS [Reads],
    [num_of_writes] AS [Writes],
    [ReadLatency(ms)] =
        CASE WHEN [num_of_reads] = 0
            THEN 0 ELSE ([io_stall_read_ms] / [num_of_reads]) END,
    [WriteLatency(ms)] =
        CASE WHEN [num_of_writes] = 0
            THEN 0 ELSE ([io_stall_write_ms] / [num_of_writes]) END,
    -- [Latency] = CASE WHEN ([num_of_reads] = 0 AND [num_of_writes] = 0)
            -- THEN 0 ELSE ([io_stalls_ms] / ([num_of_reads] + [num_of_writes])) END,
    [AvgBPerRead] =
        CASE WHEN [num_of_reads] = 0
            THEN 0 ELSE ([num_of_bytes_read] / [num_of_reads]) END,
    [AvgBPerWrite] =
        CASE WHEN [num_of_writes] = 0
            THEN 0 ELSE ([num_of_bytes_written] / [num_of_writes]) END,
    -- [AvgBPerTransfer] = CASE WHEN ([num_of_reads] = 0 AND [num_of_writes] = 0)
            -- THEN 0 ELSE
                -- (([num_of_bytes_read] + [num_of_bytes_written]) / ([num_of_reads] + [num_of_writes])) END,
    [physical_name]
FROM [DiffLatencies]
ORDER BY [WriteLatency(ms)] DESC;
'
*/
set @sqlstring='
;WITH 
time_startAnalyze_cte as
(select [file_handle], [database_id], [file_id], [DatabaseName], [avg_read_latency_ms], [avg_write_latency_ms]
      ,[avg_io_latency_ms], [File_Size_MB], [physical_name], [type_desc], [io_stall_read_ms], [num_of_reads], [io_stall_write_ms]
      ,[num_of_writes], [io_stalls_ms], [total_io],[num_of_bytes_read],[num_of_bytes_written], [collection_date]
	  FROM ['+@DatabaseAnalyze+'].['+@SchemaAnalyze+'].['+@TableAnalyze+'] 
	  where collection_date='''+cast (@time_startAnalyze_real as nvarchar(30))+'''),
time_endAnalyze_cte as
(select [file_handle], [database_id], [file_id], [DatabaseName], [avg_read_latency_ms], [avg_write_latency_ms]
      ,[avg_io_latency_ms], [File_Size_MB], [physical_name], [type_desc], [io_stall_read_ms], [num_of_reads], [io_stall_write_ms]
      ,[num_of_writes], [io_stalls_ms],[total_io],[num_of_bytes_read],[num_of_bytes_written],[collection_date]
	  FROM ['+@DatabaseAnalyze+'].['+@SchemaAnalyze+'].['+@TableAnalyze+'] 
	  where collection_date='''+cast (@time_endAnalyze_real as nvarchar(30))+'''),
[DiffLatencies] AS
(SELECT
-- Files that weren''t in the first snapshot
        [ts2].[database_id],
        [ts2].[file_id],
		[ts2].[physical_name],
		[ts2].[type_desc],
        [ts2].[num_of_reads],
        [ts2].[io_stall_read_ms],
        [ts2].[num_of_writes],
        [ts2].[io_stall_write_ms],
        [ts2].[io_stalls_ms],
        [ts2].[num_of_bytes_read],
        [ts2].[num_of_bytes_written]
    FROM time_endAnalyze_cte AS [ts2]
    LEFT OUTER JOIN time_startAnalyze_cte AS [ts1]
        ON [ts2].[file_handle] = [ts1].[file_handle]
    WHERE [ts1].[file_handle] IS NULL
UNION
SELECT
-- Diff of latencies in both snapshots
        [ts2].[database_id],
        [ts2].[file_id],
		[ts2].[physical_name],
		[ts2].[type_desc],
        [ts2].[num_of_reads] - [ts1].[num_of_reads] AS [num_of_reads],
        [ts2].[io_stall_read_ms] - [ts1].[io_stall_read_ms] AS [io_stall_read_ms],
        [ts2].[num_of_writes] - [ts1].[num_of_writes] AS [num_of_writes],
        [ts2].[io_stall_write_ms] - [ts1].[io_stall_write_ms] AS [io_stall_write_ms],
        [ts2].[io_stalls_ms] - [ts1].[io_stalls_ms] AS [io_stall],
        [ts2].[num_of_bytes_read] - [ts1].[num_of_bytes_read] AS [num_of_bytes_read],
        [ts2].[num_of_bytes_written] - [ts1].[num_of_bytes_written] AS [num_of_bytes_written]
    FROM time_endAnalyze_cte AS [ts2]
    LEFT OUTER JOIN time_startAnalyze_cte AS [ts1]
        ON [ts2].[file_handle] = [ts1].[file_handle]
    WHERE [ts1].[file_handle] IS NOT NULL)
SELECT
    DB_NAME (database_id) AS [DB],
    LEFT ([physical_name], 2) AS [Drive],
    [type_desc],
    [num_of_reads] AS [Reads],
    [num_of_writes] AS [Writes],
    [ReadLatency(ms)] =
        CASE WHEN [num_of_reads] = 0
            THEN 0 ELSE ([io_stall_read_ms] / [num_of_reads]) END,
    [WriteLatency(ms)] =
        CASE WHEN [num_of_writes] = 0
            THEN 0 ELSE ([io_stall_write_ms] / [num_of_writes]) END,
    -- [Latency] = CASE WHEN ([num_of_reads] = 0 AND [num_of_writes] = 0)
            -- THEN 0 ELSE ([io_stalls_ms] / ([num_of_reads] + [num_of_writes])) END,
    [AvgBPerRead] =
        CASE WHEN [num_of_reads] = 0
            THEN 0 ELSE ([num_of_bytes_read] / [num_of_reads]) END,
    [AvgBPerWrite] =
        CASE WHEN [num_of_writes] = 0
            THEN 0 ELSE ([num_of_bytes_written] / [num_of_writes]) END,
    -- [AvgBPerTransfer] = CASE WHEN ([num_of_reads] = 0 AND [num_of_writes] = 0)
            -- THEN 0 ELSE
                -- (([num_of_bytes_read] + [num_of_bytes_written]) / ([num_of_reads] + [num_of_writes])) END,
    [physical_name]
FROM [DiffLatencies]
ORDER BY [WriteLatency(ms)] DESC;
'
--print (@sqlstring)
exec (@sqlstring)
end
GO


