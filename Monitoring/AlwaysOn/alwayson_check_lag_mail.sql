--The script checks for data backlog on secondary replicas of availability groups. 
--When data on the secondary is more than 20 GB behind, or more than 60 minutes and 20 GB behind. 
--The script sends a warning by mail. To do this, you need to insert the script into the job in SQLServer Agent, specify the valid @profile_name and @recipients.
--If necessary, the trigger parameters can be changed.
declare @text nvarchar(4000) -- execution line
declare @subject_mail nvarchar(255) --letter subject
if exists (select top 1 1 from (
SELECT ag.name AS [AG Name], ar.replica_server_name, adc.[database_name],drs.last_received_time, drs.last_redone_time, drs.redo_queue_size/1024./1024. as redo_queue_size_GB
FROM sys.dm_hadr_database_replica_states AS drs WITH (NOLOCK)
INNER JOIN sys.availability_databases_cluster AS adc WITH (NOLOCK)
ON drs.group_id = adc.group_id
AND drs.group_database_id = adc.group_database_id
INNER JOIN sys.availability_groups AS ag WITH (NOLOCK)
ON ag.group_id = drs.group_id
INNER JOIN sys.availability_replicas AS ar WITH (NOLOCK)
ON drs.group_id = ar.group_id
AND drs.replica_id = ar.replica_id
where (drs.redo_queue_size/1024./1024.>20) or (datediff(mi, drs.last_redone_time, drs.last_received_time  )>60 and drs.redo_queue_size/1024./1024.>20)
) as x)
begin
	set @subject_mail = 'Warning!!!! Backlog of replicas on the server '+(select @@servername)
	set @text='set nocount on;
	select ''<table border="1" bordercolor="black" cellspacing="0" cellpadding="1">'';
	select ''<tr>'', ''<td>'', ''AG Name'', ''</td>'', ''<td>'', ''Server replicas '', ''</td>'', ''<td>'', ''Èלÿ בה '', ''</td>'', ''<td>'', ''Data commit time on secondary replica'', ''</td>'', ''<td>'',	''Data freshness on the secondary replica'', ''</td>'', 
	''<td>'', 	''GB lag in data file'', ''</td>'', ''<td>'', 	''Minutes behind '', ''</td>'', ''</tr>'';
	select ''<tr>'', ''<td>'', ag.name, ''</td>'', ''<td>'', ar.replica_server_name, ''</td>'', ''<td>'', adc.[database_name], ''</td>'', ''<td>'', drs.last_received_time, ''</td>'',  ''<td>'', drs.last_redone_time, ''</td>'',  
	 ''<td>'', cast (drs.redo_queue_size/1024./1024. as DECIMAL(5,2)), ''</td>'', ''<td>'', datediff(mi, drs.last_redone_time, drs.last_received_time ), ''</td>'',  ''</tr>''
		FROM sys.dm_hadr_database_replica_states AS drs WITH (NOLOCK)
	INNER JOIN sys.availability_databases_cluster AS adc WITH (NOLOCK)
	ON drs.group_id = adc.group_id
	AND drs.group_database_id = adc.group_database_id
	INNER JOIN sys.availability_groups AS ag WITH (NOLOCK)
	ON ag.group_id = drs.group_id
	INNER JOIN sys.availability_replicas AS ar WITH (NOLOCK)
	ON drs.group_id = ar.group_id
	AND drs.replica_id = ar.replica_id
	where (drs.redo_queue_size/1024./1024.>20) or (datediff(mi, drs.last_redone_time, drs.last_received_time  )>60)
	select ''</table>''';
	EXEC msdb.dbo.sp_send_dbmail
		@profile_name = 'Profile',
		@recipients = 'email',  
		@query = @text,
		@subject = @subject_mail, 
		@query_no_truncate=1,
		@importance = High,
		@body_format='HTML';
	
end
else
	print 'Ïףסעמ :)'