USE [master]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF OBJECT_ID('dbo.io_db_file_latency') IS NULL
  EXEC ('CREATE PROCEDURE dbo.io_db_file_latency AS RETURN 0;');
GO

ALTER PROCEDURE [dbo].[io_db_file_latency]
(
@DatabaseOutput NVARCHAR(300),
@SchemaOutput NVARCHAR(300),
@io_db_file_latency NVARCHAR(300),
@OutputTABLERetentionDays TINYINT = 7
)
as
/*============================================================================
  File:     io_db_file_latency.sql
  
  Summary:  Data collection io latency each database each file data on SQL Server.
  
  SQL Server Versions: 2014 onwards
------------------------------------------------------------------------------
 Written by Pavel A. Polikov
 The original script is taken from
-- SQL Server 2019 Diagnostic Information Queries
-- Glenn Berry 
-- Last Modified: December 4, 2019
-- https://www.sqlskills.com/blogs/glenn/
-- Shows you the drive-level latency for reads and writes, in milliseconds
-- Latency above 30-40ms is usually a problem
-- These latency numbers include all file activity against all SQL Server 
-- database files on each drive since SQL Server was last started
-- Calculates average latency per read, per write, and per total input/output for each database file  (Query 29) (IO Latency by File)
   

   Example: 
   exec dbo.io_db_file_latency @DatabaseOutput='msdb', @SchemaOutput='dbo', @io_db_file_latency='monitor_io_db_file_latency',@OutputTABLERetentionDays =21

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
begin
declare @sqlstring NVARCHAR (max),
		@OutputTABLECleanupDate datetime,
		@collection_date datetime
select @OutputTABLECleanupDate =  CAST( (DATEADD(DAY, -1 * @OutputTABLERetentionDays, GETDATE() ) ) AS DATETIME)
SET @collection_date = getdate()
SET @sqlstring='if not exists (select * from ['+@DatabaseOutput+'].sys.objects where name ='''+@io_db_file_latency+''' and schema_id =(select schema_id from ['+@DatabaseOutput+'].sys.schemas where name ='''+@SchemaOutput+''') )
begin
USE ['+@DatabaseOutput+'];
SET ansi_nulls on;
SET quoted_identifier on;
CREATE TABLE ['+@SchemaOutput+'].['+@io_db_file_latency+'](
	[id] int  IDENTITY (1,1) NOT NULL
      , CONSTRAINT PK_'+@SchemaOutput+''+@io_db_file_latency+'_id PRIMARY KEY CLUSTERED (id), 
	[file_handle] varbinary (max),
	[database_id] smallint,
	[file_id] smallint,
	[DatabaseName] sysname,
	[avg_read_latency_ms] NUMERIC(10,1),
	[avg_write_latency_ms] NUMERIC(10,1),
	[avg_io_latency_ms] NUMERIC(10,1),
	[File_Size_MB] NUMERIC(10,2),
	[physical_name] nvarchar(260),
	[type_desc] nvarchar(60),
	[io_stall_read_ms] bigint,
	[num_of_reads] bigint,
	[io_stall_write_ms] bigint,
	[num_of_writes] bigint, 
	[io_stalls_ms] bigint,
	[total_io] bigint,
	[io_stall_queued_read_ms] bigint,
	[io_stall_queued_wtite_ms] bigint,
	[num_of_bytes_read] bigint,
	[num_of_bytes_written] bigint,
	[collection_date] datetime
	) on [primary];
	 
	CREATE NONCLUSTERED INDEX [IX_collection_date] ON ['+@SchemaOutput+'].['+@io_db_file_latency+']
(
	[collection_date] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF);
end	'
EXEC (@sqlstring)
--print @sqlstring
SET @sqlstring='insert into ['+@DatabaseOutput+'].['+@SchemaOutput+'].['+@io_db_file_latency+'] 
(fs.[file_handle],[database_id],[file_id],[DatabaseName], [avg_read_latency_ms], [avg_write_latency_ms] , [avg_io_latency_ms],
	[File_Size_MB], [physical_name], [type_desc], [io_stall_read_ms], [num_of_reads],
	[io_stall_write_ms], [num_of_writes], [io_stalls_ms], [total_io], [io_stall_queued_read_ms], [io_stall_queued_wtite_ms],
	[num_of_bytes_read], [num_of_bytes_written],[collection_date])
SELECT [file_handle],fs.[database_id],fs.[file_id],DB_NAME(fs.database_id) AS [Database Name], CAST(fs.io_stall_read_ms/(1.0 + fs.num_of_reads) AS NUMERIC(10,1)) AS [avg_read_latency_ms],
CAST(fs.io_stall_write_ms/(1.0 + fs.num_of_writes) AS NUMERIC(10,1)) AS [avg_write_latency_ms],
CAST((fs.io_stall_read_ms + fs.io_stall_write_ms)/(1.0 + fs.num_of_reads + fs.num_of_writes) AS NUMERIC(10,1)) AS [avg_io_latency_ms],
CONVERT(DECIMAL(18,2), mf.size/128.0) AS [File Size (MB)], mf.physical_name, mf.type_desc, fs.io_stall_read_ms, fs.num_of_reads, 
fs.io_stall_write_ms, fs.num_of_writes, fs.io_stall_read_ms + fs.io_stall_write_ms AS [io_stalls_ms], fs.num_of_reads + fs.num_of_writes AS [total_io],
io_stall_queued_read_ms, io_stall_queued_write_ms, num_of_bytes_read, num_of_bytes_written,
'''+cast (@collection_date as nvarchar(30))++'''
FROM sys.dm_io_virtual_file_stats(null,null) AS fs
INNER JOIN sys.master_files AS mf WITH (NOLOCK)
ON fs.database_id = mf.database_id
AND fs.[file_id] = mf.[file_id]
ORDER BY  avg_io_latency_ms DESC OPTION (RECOMPILE)'
EXEC (@sqlstring)
--print @sqlstring
--cleanup old data
SET @sqlstring='delete from ['+@DatabaseOutput+']. ['+@SchemaOutput+'].['+@io_db_file_latency+']
where collection_date<='''+cast (@OutputTABLECleanupDate as nvarchar(30))+''''
EXEC (@sqlstring)
--print @sqlstring
end