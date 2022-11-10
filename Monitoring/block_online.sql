--shows locks on the server, sorts by the number of blocked processes
IF OBJECT_ID (N'tempdb..#dba_dm_exec_requests') IS NOT NULL 
DROP TABLE #dba_dm_exec_requests
GO
CREATE TABLE [#dba_dm_exec_requests](
	[session_id] [SMALLINT] NOT NULL,
	[start_time] [DATETIME] NOT NULL,
	[host_name] NVARCHAR(128) ,
	login_name  NVARCHAR(128) NOT NULL,
	event_info NVARCHAR(MAX) null,
	[status] [NVARCHAR](30) NOT NULL,
	[program_name] NVARCHAR(128),
	writes BIGINT,
	reads BIGINT, 
	logical_reads BIGINT,
	[blocking_session_id] [SMALLINT] NULL,
	[count_block] [SMALLINT] NULL,
	[wait_type] [NVARCHAR](60) NULL,
	[wait_time] [INT] NULL,
	[wait_resource] [NVARCHAR](256) NULL,
	[collection_time] DATETIME NOT NULL--,
) 
GO

IF OBJECT_ID (N'tempdb..#dba_dm_exec_requests_only_block') IS NOT NULL 
DROP TABLE #dba_dm_exec_requests_only_block
go
CREATE TABLE [#dba_dm_exec_requests_only_block](
	[session_id] [SMALLINT] NOT NULL,
	[start_time] [DATETIME] NOT NULL,
	[host_name] NVARCHAR(128) ,
	login_name  NVARCHAR(128) NOT NULL,
	event_info NVARCHAR(MAX) null,
	[status] [NVARCHAR](30) NOT NULL,
	[program_name] NVARCHAR(128),
	writes BIGINT,
	reads BIGINT, 
	logical_reads BIGINT,
	[blocking_session_id] [SMALLINT] NULL,
	[count_block] [SMALLINT] NULL,
	[wait_type] [NVARCHAR](60) NULL,
	[wait_time] [INT] NULL,
	[wait_resource] [NVARCHAR](256) NULL,
	[collection_time] DATETIME NOT NULL--,
) 
GO




INSERT INTO [#dba_dm_exec_requests](
	[session_id],
	[start_time],
	[host_name],
	login_name,
	event_info,
	[status],
	[program_name],
	writes,
	reads, 
	logical_reads,
	[blocking_session_id],
	[wait_type],
	[wait_time],
	[wait_resource],
	[collection_time]
)
SELECT s.session_id, 
r.start_time, 
s.host_name, 
s.login_name,
i.event_info,
r.status,
s.program_name,
r.writes,
r.reads,
r.logical_reads,
r.blocking_session_id,
r.wait_type,
r.wait_time,
r.wait_resource,
GETDATE() AS collection_time
FROM sys.dm_exec_requests AS r
JOIN sys.dm_exec_sessions AS s
 ON s.session_id = r.session_id
CROSS APPLY sys.dm_exec_input_buffer(s.session_id, r.request_id) AS i
WHERE s.session_id != @@SPID
AND s.session_id >50;

INSERT INTO [#dba_dm_exec_requests](
	[session_id],
	[start_time],
	[host_name],
	login_name,
	event_info,
	[status],
	[program_name],
	writes,
	reads, 
	logical_reads,
	[blocking_session_id],
	[wait_type],
	[wait_time],
	[wait_resource],
	[collection_time]
)

SELECT s1.session_id,  s1.last_request_start_time, s1.host_name, s1.login_name, null, s1.status, s1.program_name, s1.writes, s1.reads, s1.logical_reads,
null, null, null, null, GETDATE() AS collection_time
FROM sys.dm_exec_sessions AS s1
where s1.session_id in (select blocking_session_id from #dba_dm_exec_requests where blocking_session_id =s1.session_id) and s1.session_id not in (select session_id from #dba_dm_exec_requests where session_id =s1.session_id )

INSERT INTO [#dba_dm_exec_requests_only_block](
	[session_id],
	[start_time],
	[host_name],
	login_name,
	event_info,
	[status],
	[program_name],
	writes,
	reads, 
	logical_reads,
	[blocking_session_id],
	[wait_type],
	[wait_time],
	[wait_resource],
	[collection_time]
)
SELECT 	[session_id],
	[start_time],
	[host_name],
	login_name,
	event_info,
	[status],
	[program_name],
	writes,
	reads, 
	logical_reads,
	[blocking_session_id],
	[wait_type],
	[wait_time],
	[wait_resource],
	[collection_time]
FROM [#dba_dm_exec_requests]
WHERE (blocking_session_id<>0 OR session_id IN (SELECT blocking_session_id FROM [#dba_dm_exec_requests]))

;WITH recursive_cte (session_id,blocking_session_id)
AS 
(
SELECT DISTINCT session_id,blocking_session_id
FROM [#dba_dm_exec_requests_only_block]

UNION ALL
SELECT e.session_id,d.blocking_session_id
FROM [#dba_dm_exec_requests_only_block] e
INNER JOIN recursive_cte d
ON d.session_id=e.blocking_session_id
WHERE d.blocking_session_id!=0
)

SELECT der.session_id, 
der.login_name,
der.blocking_session_id,
count ( cte.blocking_session_id) AS leader_block2,
der.start_time, 
der.host_name, 
der.event_info,
der.program_name
--der.status,

--der.writes,
--der.reads,
--der.logical_reads,
--der.wait_type,
--der.wait_time,
--der.wait_resource,
--der.collection_time
--FROM [#dba_dm_exec_requests_only_block] der
FROM (SELECT DISTINCT session_id, 
login_name,
blocking_session_id,
start_time, 
host_name, 
event_info,
program_name
FROM [#dba_dm_exec_requests_only_block]) der
LEFT JOIN recursive_cte cte
ON der.session_id=cte.blocking_session_id AND cte.blocking_session_id!=0

GROUP BY  der.session_id, 
der.login_name,
der.blocking_session_id,
der.start_time, 
der.host_name, 
der.event_info,
der.program_name
--der.status,

--der.writes,
--der.reads,
--der.logical_reads,
--der.wait_type,
--der.wait_time,
--der.wait_resource,
--der.collection_time

ORDER BY leader_block2 DESC


OPTION (RECOMPILE);
IF OBJECT_ID (N'tempdb..#dba_dm_exec_requests_only_block') IS NOT NULL 
DROP TABLE #dba_dm_exec_requests_only_block
GO
IF OBJECT_ID (N'tempdb..#dba_dm_exec_requests') IS NOT NULL 
DROP TABLE #dba_dm_exec_requests
go
