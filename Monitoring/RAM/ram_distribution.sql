USE [master]
GO

/****** Object:  StoredProcedure [dbo].[ram_distribution]    Script Date: 17.11.2022 12:24:17 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

/*============================================================================
  File:     ram_distribution.sql
  
  Summary:  Data collection on SQL Server memory allocation.
  
  SQL Server Versions: 2014 onwards
------------------------------------------------------------------------------
  Written by Pavel A. Polikov
  https://github.com/PahanDba/mssql_dba/tree/master
 
  The original script is taken from
  https://stackoverflow.com/questions/64702115/slow-performance-in-sql-2014-in-select-without-where-claUSE
 
  Last update 17.11.2022
  
  Example exec dbo.ram_distribution @DatabaseOutput='msdb',@SchemaOutput='dbo',@PerfOutput='monitor_perfomance',@MemconfigOutput='monitor_memconfig',@MemdistribOutput='monitor_memdistrib', @OutputTABLERetentionDays=14

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
CREATE PROCEDURE [dbo].[ram_distribution]
(
@DatabaseOutput NVARCHAR(300), 
@SchemaOutput NVARCHAR(300),
@PerfOutput NVARCHAR(300),
@MemconfigOutput NVARCHAR(300),
@MemdistribOutput NVARCHAR(300),
@OutputTABLERetentionDays TINYINT = 7
)
as
begin
declare @sqlstring NVARCHAR (max),
		@OutputTABLECleanupDate datetime
select @OutputTABLECleanupDate =  CAST( (DATEADD(DAY, -1 * @OutputTABLERetentionDays, GETDATE() ) ) AS DATETIME)
SET @sqlstring='if not exists (select * from ['+@DatabaseOutput+'].sys.objects where name ='''+@PerfOutput+''' and schema_id =(select schema_id from ['+@DatabaseOutput+'].sys.schemas where name ='''+@SchemaOutput+''') )
begin
USE ['+@DatabaseOutput+'];
SET ansi_nulls on;
SET quoted_identifier on;
CREATE TABLE ['+@SchemaOutput+'].['+@PerfOutput+'](
	row# tinyint not null,
	countername NVARCHAR(128) not null,
	value int not null,
	RecommendedMinimum int null,
	collection_date datetime
	) on [primary];
	CREATE NONCLUSTERED INDEX [IX_collection_date] ON ['+@SchemaOutput+'].['+@PerfOutput+']
(
	[collection_date] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)

end	'
EXEC (@sqlstring)
SET @sqlstring='if not exists (select * from ['+@DatabaseOutput+'].sys.objects where name ='''+@MemconfigOutput+''' and schema_id =(select schema_id from ['+@DatabaseOutput+'].sys.schemas where name ='''+@SchemaOutput+''') )
begin
USE ['+@DatabaseOutput+'];
SET ansi_nulls on;
SET quoted_identifier on;
CREATE TABLE ['+@SchemaOutput+'].['+@MemconfigOutput+'](
	Row# tinyint,
	min_server_mb decimal (15,2) not null,
	max_server_mb decimal (15,2) not null,
	target_mb decimal (15,2) not null,
	total_mb decimal (15,2) not null,
	physical_mb decimal (15,2) not null,
	locked_pages_mb decimal (15,2) not null,
	collection_date datetime
	) on [primary];
		CREATE NONCLUSTERED INDEX [IX_collection_date] ON ['+@SchemaOutput+'].['+@MemconfigOutput+']
(
	[collection_date] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
end	'
EXEC (@sqlstring)
SET @sqlstring='if not exists (select * from ['+@DatabaseOutput+'].sys.objects where name ='''+@MemdistribOutput+''' and schema_id =(select schema_id from ['+@DatabaseOutput+'].sys.schemas where name= '''+@SchemaOutput+''') )
begin
USE ['+@DatabaseOutput+'];
SET ansi_nulls on;
SET quoted_identifier on;
CREATE TABLE ['+@SchemaOutput+'].['+@MemdistribOutput+'](
	row# tinyint not null,
	countername NVARCHAR(128) not null,
	memorymb decimal (15,2) null,
	prc_ofparent decimal (5,2) null,
	prc_oftarget decimal (5,2) null,
	collection_date datetime
	) on [primary];
	CREATE NONCLUSTERED INDEX [IX_collection_date] ON ['+@SchemaOutput+'].['+@MemdistribOutput+']
(
	[collection_date] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
end	'
EXEC (@sqlstring)
SET @sqlstring='
declare @collection_date datetime
SET @collection_date = getdate()
DECLARE @OtherClerksTop INT
SET @OtherClerksTop = 5

SET nocount on 
SET TRANSACTIon ISOLATIon LEVEL READ UNCOMMITTED
SET LOCK_TIMEOUT 10000

DECLARE @sql NVARCHAR(max)
DECLARE @Version VARCHAR(100)
DECLARE @ServiceName NVARCHAR(100)

SET @Version = CAST(SERVERPROPERTY(''ProductVersion'') AS VARCHAR(100))
SET @ServiceName = CASE WHEN @@SERVICENAME = ''MSSQLSERVER''
                            THEN ''SQLServer:''
                        ELSE ''MSSQL$'' + @@SERVICENAME + '':''
                        END

DECLARE @Perf TABLE (object_name NVARCHAR(20), counter_name NVARCHAR(128), instance_name NVARCHAR(128), cntr_value BIGINT, formatted_value NUMERIC(20, 2), shortname NVARCHAR(20))
INSERT INTO @Perf(object_name, counter_name, instance_name, cntr_value, formatted_value, shortname)
SELECT 
  CASE 
    WHEN CHARINDEX (''Memory Manager'', object_name)> 0 THEN ''Memory Manager''
    WHEN CHARINDEX (''Buffer Manager'', object_name)> 0 THEN ''Buffer Manager''
    WHEN CHARINDEX (''Plan Cache'', object_name)> 0 THEN ''Plan Cache''
    WHEN CHARINDEX (''Buffer Node'', object_name)> 0 THEN ''Buffer Node'' -- 2008
    WHEN CHARINDEX (''Memory Node'', object_name)> 0 THEN ''Memory Node'' -- 2012
    WHEN CHARINDEX (''Cursor'', object_name)> 0 THEN ''Cursor''
    WHEN CHARINDEX (''Databases'', object_name) > 0 THEN ''Databases''
    ELSE NULL 
  END AS object_name,
  CAST(RTRIM(counter_name) AS NVARCHAR(100)) AS counter_name, 
  RTRIM(instance_name) AS instance_name, 
  cntr_value,
  CAST(NULL AS DECIMAL(20,2)) AS formatted_value,
  SUBSTRING(counter_name,  1, PATINDEX(''% %'', counter_name)) shortname
FROM sys.dm_os_performance_counters 
WHERE (object_name LIKE @ServiceName + ''Buffer Node%''    -- LIKE is faster than =. I have no idea why
    OR object_name LIKE @ServiceName + ''Buffer Manager%'' 
    OR object_name LIKE @ServiceName + ''Memory Node%'' 
    OR object_name LIKE @ServiceName + ''Plan Cache%'')
  AND (counter_name LIKE ''%pages %'' 
    OR counter_name LIKE ''%Node Memory (KB)%''
    OR counter_name = ''Page life expectancy'' 
    )
    OR  (object_name = @ServiceName + ''Memory Manager''
        AND counter_name IN (''Granted Workspace Memory (KB)'', ''Maximum Workspace Memory (KB)'',
                                    ''Memory Grants Outstanding'',     ''Memory Grants Pending'',
                                    ''Target Server Memory (KB)'',     ''Total Server Memory (KB)'',
                                    ''Cnecti Memory (KB)'',        ''Lock Memory (KB)'',
                                    ''Optimizer Memory (KB)'',         ''SQL Cache Memory (KB)'',
                                    -- for 2012
                                    ''Free Memory (KB)'',              ''Reserved Server Memory (KB)'',
                                    ''Database Cache Memory (KB)'',    ''Stolen Server Memory (KB)'',
                                    -- XTP
                                    ''Log Pool Memory (KB)'')
      )
    OR (object_name LIKE @ServiceName + ''Cursor Manager by Type%''
      AND counter_name = ''Cursor memory usage''
      AND instance_name = ''_Total''

      )
-- Add UNIONt to ''Cursor memory usage''
UPDATE @Perf
SET counter_name = counter_name + '' (KB)''
WHERE counter_name = ''Cursor memory usage'' 

-- Cvert values from pages and KB to MB and rename counters accordingly
UPDATE @Perf
SET 
  counter_name = REPLACE(REPLACE(REPLACE(counter_name, '' pages'', ''''), '' (KB)'', ''''), '' (MB)'', ''''), 
  formatted_value = 
  CASE 
    WHEN counter_name LIKE ''%pages'' THEN cntr_value/128. 
    WHEN counter_name LIKE ''%(KB)'' THEN cntr_value/1024. 
    ELSE cntr_value
  END

-- Delete some pre 2012 counters for 2012 in order to remove duplicates
DELETE P2008
FROM @Perf P2008
INNER JOIN @Perf P2012  
   on replace(P2008.object_name, ''Buffer'', ''Memory'') = P2012.object_name AND P2008.shortname = P2012.shortname
WHERE P2008.object_name IN (''Buffer Manager'', ''Buffer Node'')

-- Update counter/object names so they look like in 2012
UPDATE PC
SET 
  object_name = REPLACE(object_name, ''Buffer'', ''Memory''),
  counter_name = ISNULL(M.NewName, counter_name)  
FROM @Perf PC
  LEFT JOIN
  (
    SELECT ''Free'' AS OldName, ''Free Memory'' AS NewName UNION ALL
    SELECT ''Database'', ''Database Cache Memory'' UNION ALL
    SELECT ''Stolen'', ''Stolen Server Memory'' UNION ALL
    SELECT ''Reserved'', ''Reserved Server Memory'' UNION ALL
    SELECT ''Foreign'', ''Foreign Node Memory''
  ) M ON M.OldName = PC.counter_name
  AND NewName NOT IN (SELECT counter_name FROM @Perf WHERE object_name = ''Memory Manager'') 
WHERE object_name IN (''Buffer Manager'', ''Buffer Node'')

-- Add Memory Clerks

-- Add some Memory Clerk descriptis
IF OBJECT_ID(''tempdb..#mem_clerks_desc'') IS NOT NULL DROP TABLE #mem_clerks_desc
CREATE TABLE #mem_clerks_desc(type varchar(60), descripti varchar(60), is_perf_counter bit)
INSERT #mem_clerks_desc VALUES(''CACHESTORE_BROKERTO'',''Service Broker Transmissi Object Cache'', 0)
INSERT #mem_clerks_desc VALUES(''CACHESTORE_COLUMNSTOREOBJECTPOOL'',''Column Store Object Pool'', 0)
INSERT #mem_clerks_desc VALUES(''CACHESTORE_OBJCP'',''Object Plans'', 1)
INSERT #mem_clerks_desc VALUES(''CACHESTORE_PHDR'',''Bound Trees'', 1)
INSERT #mem_clerks_desc VALUES(''CACHESTORE_SEHOBTCOLUMNATTRIBUTE'',''SE Shared Column Metadata Cache'', 0)
INSERT #mem_clerks_desc VALUES(''CACHESTORE_SQLCP'',''SQL Plans'', 1)
INSERT #mem_clerks_desc VALUES(''CACHESTORE_SYSTEMROWSET'',''System RowSET Store'', 0)
INSERT #mem_clerks_desc VALUES(''CACHESTORE_TEMPTABLES'',''Temporary TABLEs & TABLE Variables'', 1)
INSERT #mem_clerks_desc VALUES(''CACHESTORE_XPROC'',''Extended Stored Procedures'', 1)
INSERT #mem_clerks_desc VALUES(''MEMORYCLERK_QUERYDISKSTORE'',''Query Store Memory'', 0)
INSERT #mem_clerks_desc VALUES(''MEMORYCLERK_QUERYDISKSTORE_HASHMAP'',''Query Store Hash TABLE '', 0)
INSERT #mem_clerks_desc VALUES(''MEMORYCLERK_SOSMEMMANAGER'',''SOS Memory Manager'', 0)
INSERT #mem_clerks_desc VALUES(''MEMORYCLERK_SOSNODE'',''SOS Node'', 0)
INSERT #mem_clerks_desc VALUES(''MEMORYCLERK_SOSOS'',''SOS Memory Clerk'', 0)
INSERT #mem_clerks_desc VALUES(''MEMORYCLERK_SQLBUFFERPOOL'',''Database Cache Memory'', 1)
INSERT #mem_clerks_desc VALUES(''MEMORYCLERK_SQLCLR'',''SQL CLR'', 0)
INSERT #mem_clerks_desc VALUES(''MEMORYCLERK_SQLCLRASSEMBLY'',''SQL CLR Assembly'', 0)
INSERT #mem_clerks_desc VALUES(''MEMORYCLERK_SQLCNECTIPOOL'',''Cnecti Memory'', 1)
INSERT #mem_clerks_desc VALUES(''MEMORYCLERK_SQLGENERAL'',''SQL General'', 0)
INSERT #mem_clerks_desc VALUES(''MEMORYCLERK_SQLLOGPOOL'',''Log Pool Memory'', 1)
INSERT #mem_clerks_desc VALUES(''MEMORYCLERK_SQLOPTIMIZER'',''Optimizer Memory'', 1)
-- INSERT #mem_clerks_desc VALUES(''MEMORYCLERK_SQLQERESERVATIS'',''Granted Workspace Memory (USEd+Reserved)'', 0) -- Exclude completely
INSERT #mem_clerks_desc VALUES(''MEMORYCLERK_SQLQUERYEXEC'',''SQL Query EXEC'', 0)
INSERT #mem_clerks_desc VALUES(''MEMORYCLERK_SQLQUERYPLAN'',''SQL Query Plan'', 0)
INSERT #mem_clerks_desc VALUES(''MEMORYCLERK_SQLSERVICEBROKER'',''SQL Service Broker'', 0)
INSERT #mem_clerks_desc VALUES(''MEMORYCLERK_SQLSTORENG'',''SQL Storage Engine'', 0)
INSERT #mem_clerks_desc VALUES(''MEMORYCLERK_SQLTRACE'',''SQL Trace'', 0)
INSERT #mem_clerks_desc VALUES(''MEMORYCLERK_XE'',''Extended Events Engine'', 0)
INSERT #mem_clerks_desc VALUES(''MEMORYCLERK_XE_BUFFER'',''Extended Events Buffer'', 0)
INSERT #mem_clerks_desc VALUES(''MEMORYCLERK_XTP'',''In-Memory objects (XTP)'', 0)
INSERT #mem_clerks_desc VALUES(''OBJECTSTORE_LOCK_MANAGER'',''Lock Memory'', 1)
INSERT #mem_clerks_desc VALUES(''OBJECTSTORE_SERVICE_BROKER'',''Service Broker (Object Store)'', 0)
INSERT #mem_clerks_desc VALUES(''OBJECTSTORE_SNI_PACKET'',''SNI Packet (Object Store)'', 0)
INSERT #mem_clerks_desc VALUES(''OBJECTSTORE_XACT_CACHE'',''Transactis Cache (Object Store)'', 0)
INSERT #mem_clerks_desc VALUES(''USERSTORE_DBMETADATA'',''Database Metadata (USEr Store)'', 0)
INSERT #mem_clerks_desc VALUES(''USERSTORE_OBJPERM'',''Object Permissis (USEr Store)'', 0)
INSERT #mem_clerks_desc VALUES(''USERSTORE_SCHEMAMGR'',''Schema Manager (USEr Store)'', 0)
INSERT #mem_clerks_desc VALUES(''USERSTORE_TOKENPERM'',''Token Permissis (USEr Store)'', 0)

IF OBJECT_ID(''tempdb..#mem_clerks'') IS NOT NULL DROP TABLE #mem_clerks
CREATE TABLE #mem_clerks(type varchar(60), mem_type varchar(20), pages_mb DECIMAL(20, 2))

IF CAST(SUBSTRING(@VersiON, 1, CHARINDEX(''.'', @Version, 1)-1) AS INT) >= 11 -- SQL 2012
BEGIN
  SET @sql = ''
  SELECT type, mem_type, CAST(SUM(pages_mb)/1024. AS DECIMAL(20, 2)) as pages_mb
  FROM  
  (SELECT type, pages_kb as pages, virtual_memory_committed_kb as virtual, awe_allocated_kb as awe FROM sys.dm_os_memory_clerks WHERE type <> ''''MEMORYCLERK_SQLQERESERVATIONS'''') AS t  
  UNPIVOT (pages_mb FOR mem_type IN (pages, virtual, awe)  
  )AS unpvt
  WHERE unpvt.pages_mb > 0
  GROUP BY type, mem_type''
END
ELSE
BEGIN
  SET @sql = ''
  SELECT type, mem_type, CAST(SUM(pages_mb)/1024. AS DECIMAL(20, 2)) as pages_mb
  FROM  
  (SELECT type, single_pages_kb as single_pages, multi_pages_kb as multi_pages, virtual_memory_committed_kb as virtual, awe_allocated_kb as awe FROM sys.dm_os_memory_clerks WHERE type <> ''''MEMORYCLERK_SQLQERESERVATIS'''') AS t  
  UNPIVOT (pages_mb FOR mem_type IN (single_pages, multi_pages, virtual, awe)
  )AS unpvt
  WHERE unpvt.pages_mb > 0
  GROUP BY type, mem_type''
END

--print @sql 
INSERT #mem_clerks(type, mem_type, pages_mb)
EXEC(@SQL)

-- Build Memory Tree
DECLARE @MemTree TABLE (Id int, ParentId int, counter_name NVARCHAR(128), formatted_value NUMERIC(20, 2), shortname NVARCHAR(20))

--->>> EXTRA MEMORY (outside of the Buffer Pool) ---  ly for SQL 2008R2 and older
IF CAST(SUBSTRING(@Version, 1, CHARINDEX(''.'', @Version, 1)-1) AS INT) < 11
BEGIN
  -- Level 1: Total
  INSERT @MemTree(Id, ParentId, counter_name, formatted_value, shortname)
  SELECT 
    Id = 2000,
    ParentId = NULL,
    counter_name = ''Extra Server Memory'', 
    formatted_value = SUM(pages_mb),
    shortname = ''Extra''
  FROM #mem_clerks mc
  LEFT JOIN #mem_clerks_desc mcd  
  on mcd.type = mc.type
  WHERE mem_type = ''multi_pages''
  -- Level 2: Detailed
  INSERT @MemTree(Id, ParentId, counter_name, formatted_value, shortname)
  SELECT TOP (@OtherClerksTop)
    Id = 2100,
    ParentId = 2000,
    counter_name = ISNULL(descripti, mc.type), 
    formatted_value = pages_mb,
    shortname = ''Extra''
  FROM #mem_clerks mc
  LEFT JOIN #mem_clerks_desc mcd  
  on mcd.type = mc.type
  WHERE mem_type = ''multi_pages''
    AND pages_mb > 1.
  ORDER BY pages_mb DESC

  -- Level 2: ''Other Server Memory Extra'' = ''Server Memory (Extra)'' - SUM(Children of ''Server Memory (Extra)'')
  INSERT @MemTree(Id, ParentId, counter_name, formatted_value, shortname)
  SELECT
    Id = 2110,
    ParentId = 2000,
    counter_name = ''<Other Memory Clerks>'', 
    formatted_value = (SELECT SSM.formatted_value FROM @MemTree SSM WHERE Id = 2000) - SUM(formatted_value),
    shortname = ''Other Extra''
  FROM @MemTree 
  WHERE ParentId = 2000
END
---<<< EXTRA MEMORY (outside of the Buffer Pool) ---  ly for SQL 2008R2 and older

------ MAIN
-- Level 1
INSERT @MemTree(Id, ParentId, counter_name, formatted_value, shortname)
SELECT 
  Id = 1000,
  ParentId = NULL,
  counter_name, 
  formatted_value,
  shortname
FROM @Perf
WHERE object_name = ''Memory Manager'' AND 
  counter_name IN (''Target Server Memory'')

-- Level 2
INSERT @MemTree(Id, ParentId, counter_name, formatted_value, shortname)
SELECT
  Id = CASE WHEN counter_name = ''Maximum Workspace Memory'' THEN 1100 ELSE 1200 END,
  ParentId = 1000,
  counter_name, 
  formatted_value,
  shortname
FROM @Perf
WHERE object_name = ''Memory Manager'' AND 
  counter_name IN (''Total Server Memory'', ''Maximum Workspace Memory'') 
UNION ALL
SELECT 
  Id = 1150,
  ParentId = 1000,
  counter_name = ''Foreign Node Memory'', 
  formatted_value = SUM(formatted_value),
  shortname = ''Foreign''
FROM @Perf
WHERE object_name = ''Memory Node'' AND 
  counter_name IN (''Foreign Node Memory'')
HAVING SUM(formatted_value) > 0

-- Level 3
INSERT @MemTree(Id, ParentId, counter_name, formatted_value, shortname)
SELECT
  Id = CASE counter_name 
           WHEN ''Granted Workspace Memory'' THEN 1110 
           WHEN ''Stolen Server Memory'' THEN 1220 
           ELSE 1210
         END,
  ParentId = CASE counter_name 
               WHEN ''Granted Workspace Memory'' THEN 1100 
               ELSE 1200 
             END,
  counter_name, 
  formatted_value,
  shortname
FROM @Perf
WHERE object_name = ''Memory Manager'' 
  AND counter_name IN (''Stolen Server Memory'', ''Database Cache Memory'', ''Free Memory'', ''Granted Workspace Memory'')

-- Level 4

INSERT @MemTree(Id, ParentId, counter_name, formatted_value, shortname)
SELECT
  Id = 1225,
  ParentId = 1220,
  counter_name = p.object_name, 
  formatted_value = SUM(formatted_value) - SUM(ISNULL(mc.pages_mb, 0)), -- For SQL 2008 R2 and older subtract multi_pages
  p.shortname
FROM @Perf p
LEFT JOIN
(
  SELECT descripti as instance_name, pages_mb
  FROM #mem_clerks_desc mcd
      INNER JOIN #mem_clerks mc  
      on mc.type = mcd.type
  WHERE mc.mem_type = ''multi_pages'' -- For SQL 2008 R2 and older
    AND ISNULL(mcd.is_perf_counter, 0) = 1
) mc  
on mc.instance_name = p.instance_name 
WHERE p.object_name = ''Plan Cache'' 
  AND p.counter_name IN (''Cache'')
  AND p.instance_name <> ''_Total''
GROUP BY p.object_name, p.shortname

UNION ALL

SELECT
  Id = 1222,
  ParentId = 1220,
  p.counter_name, 
  formatted_value = p.formatted_value - ISNULL(mc.pages_mb, 0), -- For SQL 2008 R2 and older subtract multi_pages
  shortname
FROM @Perf p
LEFT JOIN
(
  SELECT descripti as counter_name, pages_mb
  FROM #mem_clerks_desc mcd
      INNER JOIN #mem_clerks mc  
      on mc.type = mcd.type
  WHERE mc.mem_type = ''multi_pages'' -- For SQL 2008 R2 and older
    AND ISNULL(mcd.is_perf_counter, 0) = 1
) mc  
on mc.counter_name = p.counter_name 
WHERE ((object_name = ''Memory Manager'' AND shortname IN (''Cnecti'', ''Lock'', ''Optimizer'', ''Log''))
  )
  AND ISNULL(formatted_value, 0) > 0

UNION ALL

SELECT  -- Memory Clerks (SQL 2008)
    Id = 1222,
    ParentId = 1220,
    T.counter_name,
    T.formatted_value,
    shortname = ''memory clerks''
FROM
(
  SELECT TOP (@OtherClerksTop)
    counter_name = ISNULL(mcd.descripti, mc.type), --  + ''  + mc.mem_type, 
    formatted_value = mc.pages_mb
  FROM #mem_clerks mc
  LEFT JOIN #mem_clerks_desc mcd  
  on mcd.type = mc.type
  WHERE CAST(SUBSTRING(@Version, 1, CHARINDEX(''.'', @Version, 1)-1) AS INT) < 11 -- SQL 2008 and older
    AND mc.mem_type = ''single_pages''  -- SQL 2008 and older
    AND ISNULL(mcd.is_perf_counter, 0) = 0
  ORDER BY pages_mb DESC
) T

UNION ALL

SELECT   -- Memory Clerks (SQL 2012+)
    Id = 1222,
    ParentId = 1220,
    T.counter_name,
    T.formatted_value,
    shortname = ''memory clerks''
FROM
(
  SELECT TOP (@OtherClerksTop)
    counter_name = ISNULL(mcd.descripti, mc.type) + CASE WHEN mc.mem_type <> ''pages'' THEN '' ('' + mc.mem_type + '')'' ELSE '''' END, 
    formatted_value = mc.pages_mb
  FROM #mem_clerks mc
  LEFT JOIN #mem_clerks_desc mcd  
  on mcd.type = mc.type
  WHERE CAST(SUBSTRING(@Version, 1, CHARINDEX(''.'', @Version, 1)-1) AS INT) >= 11 -- SQL 2012 and newer
    AND ((mc.mem_type = ''pages'' AND ISNULL(mcd.is_perf_counter, 0) = 0)
      OR mc.mem_type IN (''virtual'', ''awe'')
    )
  ORDER BY pages_mb DESC
) T

UNION ALL

SELECT
  Id = 1112,
  ParentId = 1110,
  counter_name, 
  formatted_value,
  shortname
FROM @Perf
WHERE object_name = ''Memory Manager'' 
  AND shortname IN (''Reserved'')
UNION ALL
SELECT
  Id = P.ParentID + 1,
  ParentID = P.ParentID,
  ''USEd Workspace Memory'' AS counter_name,
  SUM(USEd_memory_kb)/1024. as formatted_value,
  NULL AS shortname
FROM sys.dm_EXEC_query_resource_semaphores 
  CROSS JOIN (SELECT 1220 AS ParentID UNION ALL SELECT 1110) as P
GROUP BY P.ParentID

-- Level 4 -- ''Other Stolen Server Memory'' = ''Stolen Server Memory'' - SUM(Children of ''Stolen Server Memory'')
INSERT @MemTree(Id, ParentId, counter_name, formatted_value, shortname)
SELECT
  Id = 1222,
  ParentId = 1220,
  counter_name = ''<Other Memory Clerks>'', 
  formatted_value = (SELECT SSM.formatted_value FROM @MemTree SSM WHERE Id = 1220) - SUM(formatted_value),
  shortname = ''Other Stolen''
FROM @MemTree 
WHERE ParentId = 1220

-- Level 5
INSERT @MemTree(Id, ParentId, counter_name, formatted_value, shortname)
SELECT
  Id = CASE WHEN p.instance_name = ''SQL Plans'' THEN 1226 ELSE 1230 END,
  ParentId = 1225,
  counter_name = p.instance_name, 
  formatted_value = formatted_value - ISNULL(mc.pages_mb, 0), -- For SQL 2008 R2 and older subtract multi_pages
  p.shortname
FROM @Perf p
LEFT JOIN
(
  SELECT descripti as instance_name, pages_mb
  FROM #mem_clerks_desc mcd
      INNER JOIN #mem_clerks mc  
      on mc.type = mcd.type 
  WHERE mc.mem_type = ''multi_pages'' -- For SQL 2008 R2 and older
    AND ISNULL(mcd.is_perf_counter, 0) = 1
) mc  
on mc.instance_name = p.instance_name 
WHERE p.object_name = ''Plan Cache'' 
  AND p.counter_name IN (''Cache'')
  AND p.instance_name <> ''_Total''

-- Level 6
INSERT @MemTree(Id, ParentId, counter_name, formatted_value, shortname)
SELECT
  Id = 1227,
  ParentId = 1226,
  counter_name,
  formatted_value,
  shortname
FROM @Perf
WHERE (object_name = ''Memory Manager'' AND shortname = ''SQL'')  -- SQL Cache Memory
    OR object_name = ''Cursor''

-- Results:

-- PLE and Memory Grants
insert into ['+@DatabaseOutput+'].['+@SchemaOutput+'].['+@PerfOutput+'] (row#, countername, value, RecommendedMinimum, collection_date)
SELECT
    ROW_NUMBER() OVER(ORDER BY P.counter_name + ISNULL('' (Node: '' + NULLIF(P.instance_name, '''') + '')'', '''') desc) AS Row#,[Counter Name] = P.counter_name + ISNULL('' (Node: '' + NULLIF(P.instance_name, '''') + '')'', ''''), 
    cntr_value as Value,
    RecommendedMinimum = 
        CASE 
            WHEN P.counter_name = ''Page life expectancy'' AND R.Value <= 300 -- no less than 300
                THEN 300
            WHEN P.counter_name = ''Page life expectancy'' AND R.Value > 300 
                THEN R.Value
            ELSE NULL 
        END, @collection_date as collection_date
FROM  @Perf P
LEFT JOIN -- Recommended PLE calculatis
    (
        SELECT 
            object_name, 
            counter_name, 
            instance_name, 
            CEILING(formatted_value/4096.*5) * 60 AS Value -- 300 per every 4GB of Buffer Pool memory or around 60 secds (1 minute) per every 819MB
        FROM @Perf PD
        WHERE counter_name = ''Database Cache Memory''
    ) R  
    on R.object_name = P.object_name 
       AND R.instance_name = P.instance_name
WHERE 
  (P.object_name = ''Memory Manager''
  AND P.counter_name IN (''Memory Grants Outstanding'', ''Memory Grants Pending'', ''Page life expectancy'')
  )
    OR -- For NUMA
  (
    P.object_name = ''Memory Node'' AND P.counter_name = ''Page life expectancy''
    AND (
        SELECT COUNT(DISTINCT instance_name)
        FROM @Perf 
        WHERE object_name = ''Memory Node''
    ) > 1
  )
ORDER BY P.counter_name DESC, P.instance_name

-- Get physical memory
-- You can also extract this informati from sys.dm_os_sys_info but the column names have changed starting from 2012
IF OBJECT_ID(''tempdb..#msver'') IS NOT NULL DROP TABLE #msver
CREATE TABLE #msver(ID int, Name  sysname, Internal_Value int, Value NVARCHAR(512))
INSERT #msver EXEC master.dbo.xp_msver ''PhysicalMemory''

DECLARE @locked_page_allocations_mb DECIMAL(20, 2)
IF OBJECT_ID(''sys.dm_os_process_memory'') IS NOT NULL
  SELECT @locked_page_allocations_mb = locked_page_allocations_kb / 1024.FROM sys.dm_os_process_memory

-- Physical memory, cfig parameters and Target memory
insert into ['+@DatabaseOutput+'].['+@SchemaOutput+'].['+@MemconfigOutput+'] (Row#,min_server_mb,	max_server_mb, target_mb, total_mb, physical_mb,
	locked_pages_mb, collection_date)

SELECT 
  ROW_NUMBER() OVER(ORDER BY id ASC) AS Row#,
  min_server_mb = (SELECT CAST(value_in_USE AS DECIMAL(20, 2)) FROM sys.configurations WHERE name = ''min server memory (MB)''),
  max_server_mb = (SELECT CAST(value_in_USE AS DECIMAL(20, 2)) FROM sys.configurations WHERE name = ''max server memory (MB)''),
  target_mb = (SELECT formatted_value FROM @Perf WHERE object_name = ''Memory Manager'' AND counter_name IN (''Target Server Memory'')),
  total_mb = (SELECT formatted_value FROM @Perf WHERE object_name = ''Memory Manager'' AND counter_name IN (''Total Server Memory'')),
  physical_mb = CAST(Internal_Value AS DECIMAL(20, 2)),
  locked_pages_mb = @locked_page_allocations_mb
  , @collection_date as collection_date
FROM #msver

-- Memory tree
;WITH CTE
AS
(
  SELECT 0 as lvl, counter_name, formatted_value, Id, NULL AS ParentId, shortname, formatted_value as TargetServerMemory, CAST(NULL AS DECIMAL(20,2)) As Perc, CAST(NULL AS DECIMAL(20,2)) As PercOfTarget
  FROM @MemTree
  WHERE ParentId IS NULL
  UNION ALL
  SELECT CTE.lvl+1,
    CAST(REPLICATE('' '', 6*(CTE.lvl)) + NCHAR(124) + REPLICATE(NCHAR(183), 3) + MT.counter_name AS NVARCHAR(128)), 
    MT.formatted_value, MT.Id, MT.ParentId, MT.shortname, CTE.TargetServerMemory,
    CAST(ISNULL(100.0*MT.formatted_value/NULLIF(CTE.formatted_value, 0),0) AS DECIMAL(20,2)) AS Perc,
    CAST(ISNULL(100.0*MT.formatted_value/NULLIF(CTE.TargetServerMemory, 0),0) AS DECIMAL(20,2)) AS PercOfTarget
  FROM @MemTree MT
  INNER JOIN CTE  
  on MT.ParentId = CTE.Id
)
insert into ['+@DatabaseOutput+'].['+@SchemaOutput+'].['+@MemdistribOutput+'] (row#, countername, memorymb, prc_ofparent, prc_oftarget, collection_date)
SELECT 
  ROW_NUMBER() OVER(ORDER BY id ASC) AS Row#, counter_name AS [Counter Name], CASE WHEN formatted_value > 0 THEN formatted_value ELSE NULL END AS [Memory MB], Perc AS [% of Parent], CASE WHEN lvl >= 2 THEN PercOfTarget ELSE NULL END AS [% of Target]
  , @collection_date as collection_date
FROM CTE
ORDER BY ISNULL(Id, 10000), formatted_value DESC
'
EXEC (@sqlstring);
--cleanup old data
SET @sqlstring='delete from ['+@DatabaseOutput+']. ['+@SchemaOutput+'].['+@PerfOutput+']
where collection_date<='''+cast (@OutputTABLECleanupDate as nvarchar(30))+''''
EXEC (@sqlstring)
SET @sqlstring='delete from ['+@DatabaseOutput+']. ['+@SchemaOutput+'].['+@MemconfigOutput+']
where collection_date<='''+cast (@OutputTABLECleanupDate as nvarchar(30))+''''
EXEC (@sqlstring)
SET @sqlstring='delete from ['+@DatabaseOutput+']. ['+@SchemaOutput+'].['+@MemdistribOutput+']
where collection_date<='''+cast (@OutputTABLECleanupDate as nvarchar(30))+''''
EXEC (@sqlstring)
end
GO


