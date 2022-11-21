USE [master]
GO

/****** Object:  StoredProcedure [dbo].[ram_distribution]    Script Date: 17.11.2022 12:24:17 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

/*============================================================================
  File:     ram_distribution_analyze.sql
  
  Summary:  Analyze Data collection on SQL Server memory allocation.
  
  SQL Server Versions: 2014 onwards
------------------------------------------------------------------------------
  Written by Pavel A. Polikov
  https://github.com/PahanDba/mssql_dba

  The original script is taken from
  https://stackoverflow.com/questions/64702115/slow-performance-in-sql-2014-in-select-without-where-claUSE
 
  Last update 17.11.2022
  
  Example
  exec dbo.ram_distribution_analyze @DatabaseAnalyze='msdb',@SchemaAnalyze='dbo', @PerfAnalyze='monitor_perfomance', @MemconfigAnalyze='monitor_memconfig', @MemdistribAnalyze='monitor_memdistrib',@time_startAnalyze='2022-11-16 17:57:27',@time_endAnalyze='2022-11-18 17:57:26'  

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
alter procedure dbo.ram_distribution_analyze
(
@DatabaseAnalyze NVARCHAR(300),
@SchemaAnalyze NVARCHAR(300),
@PerfAnalyze NVARCHAR(300),
@MemconfigAnalyze NVARCHAR(300),
@MemdistribAnalyze NVARCHAR(300),
@time_startAnalyze datetime,
@time_endAnalyze datetime
)
as
begin

declare @sqlstring nvarchar(max)

set @sqlstring='SELECT  t1.[row#]
      ,t1.[countername]
      ,t1.[value]
      ,t1.[RecommendedMinimum]
      ,t1.[collection_date]
	  ,t2.[row#]
      ,t2.[countername]
      ,t2.[value]
      ,t2.[RecommendedMinimum]
      ,t2.[collection_date]
	  ,t2.[value]-t1.[value] as ''delta''
  FROM ['+@DatabaseAnalyze+'].['+@SchemaAnalyze+'].['+@PerfAnalyze+'] t1
  join ['+@DatabaseAnalyze+'].['+@SchemaAnalyze+'].['+@PerfAnalyze+'] t2
  --on t2.row#=t1.row# and t2.countername=t1.countername
  on cast(t2.row# as nvarchar(6))+t2.countername=cast(t1.row# as nvarchar(6))+t1.countername
  where t1.[collection_date]=(select top 1 collection_date
from ['+@DatabaseAnalyze+'].['+@SchemaAnalyze+'].['+@PerfAnalyze+']
where collection_date>='''+cast (@time_startAnalyze as nvarchar(30))+'''
order by collection_date asc) and t2.[collection_date]=(select top 1 collection_date
from ['+@DatabaseAnalyze+'].['+@SchemaAnalyze+'].['+@PerfAnalyze+']
where collection_date<='''+cast (@time_endAnalyze as nvarchar(30))+'''
order by collection_date desc)
  order by  t1.row# asc
'
exec (@sqlstring)
set @sqlstring=' SELECT t1.[Row#]
	   ,t1.[min_server_mb]
      ,t1.[max_server_mb]
      ,t1.[target_mb]
      ,t1.[total_mb]
      ,t1.[physical_mb]
      ,t1.[locked_pages_mb]
      ,t1.[collection_date]
	  ,t2.[Row#]
	  ,t2.[min_server_mb]
      ,t2.[max_server_mb]
      ,t2.[target_mb]
      ,t2.[total_mb]
      ,t2.[physical_mb]
      ,t2.[locked_pages_mb]
      ,t2.[collection_date]
	  ,t2.[target_mb]-t1.[target_mb] as ''delta_target_mb''
	  ,t2.[total_mb]-t1.[total_mb] as ''delta_total_mb''
	  ,t2.[locked_pages_mb]-t1.[locked_pages_mb] as ''delta_locked_pages_mb''
  FROM ['+@DatabaseAnalyze+'].['+@SchemaAnalyze+'].['+@MemconfigAnalyze+'] t1
  join ['+@DatabaseAnalyze+'].['+@SchemaAnalyze+'].['+@MemconfigAnalyze+'] t2
  on t2.Row#=t1.Row#
 where t1.[collection_date]=(select top 1 collection_date
from ['+@DatabaseAnalyze+'].['+@SchemaAnalyze+'].['+@MemconfigAnalyze+']
where collection_date>='''+cast (@time_startAnalyze as nvarchar(30))+'''
order by collection_date asc) and t2.[collection_date]=(select top 1 collection_date
from ['+@DatabaseAnalyze+'].['+@SchemaAnalyze+'].['+@MemconfigAnalyze+']
where collection_date<='''+cast (@time_endAnalyze as nvarchar(30))+'''
order by collection_date desc)
'
exec (@sqlstring)
set @sqlstring='  SELECT t1.[row#]
      ,t1.[countername]
      ,t1.[memorymb]
      ,t1.[prc_ofparent]
      ,t1.[prc_oftarget]
      ,t1.[collection_date]
	  ,t2.[row#]
      ,t2.[countername]
      ,t2.[memorymb]
      ,t2.[prc_ofparent]
      ,t2.[prc_oftarget]
      ,t2.[collection_date]
	  ,isnull(t2.[memorymb],0)-isnull(t1.[memorymb],0) as ''delta_memorymb''
  FROM ['+@DatabaseAnalyze+'].['+@SchemaAnalyze+'].['+@MemdistribAnalyze+'] t1
  join ['+@DatabaseAnalyze+'].['+@SchemaAnalyze+'].['+@MemdistribAnalyze+'] t2
  --on t2.row#=t1.Row# and t2.countername=t1.countername
  on cast(t2.row# as nvarchar(6))+t2.countername=cast(t1.Row# as nvarchar(6))+t1.countername
  where t1.[collection_date]=(select top 1 collection_date
from ['+@DatabaseAnalyze+'].['+@SchemaAnalyze+'].['+@MemdistribAnalyze+']
where collection_date>='''+cast (@time_startAnalyze as nvarchar(30))+'''
order by collection_date asc) and t2.[collection_date]=(select top 1 collection_date
from ['+@DatabaseAnalyze+'].['+@SchemaAnalyze+'].['+@MemdistribAnalyze+']
where collection_date<='''+cast (@time_endAnalyze as nvarchar(30))+'''
order by collection_date desc)'
exec (@sqlstring)
end

  
