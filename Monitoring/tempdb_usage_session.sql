--shows which process takes up space in tempdb
SELECT dtsu.session_id,
	   dess.login_name,
	   dess.host_name,
	   dess.login_time,
    SUM(dtsu.internal_objects_alloc_page_count) AS NumOfPagesAllocatedInTempDBforInternalTask,
	SUM(dtsu.internal_objects_alloc_page_count)*8/1024/1024 AS GBAllocatedInTempDBforInternalTask,
    SUM(dtsu.user_objects_alloc_page_count) AS NumOfPagesAllocatedInTempDBforUserTask,
	SUM(dtsu.user_objects_alloc_page_count)*8/1024/1024 AS GBAllocatedInTempDBforUserTask,
    SUM(dtsu.user_objects_dealloc_page_count) AS NumOfPagesDellocatedInTempDBforUserTask,
	SUM(dtsu.user_objects_dealloc_page_count)*8/1024/1024 AS GBDellocatedInTempDBforUserTask,
	GETDATE() AS collection_date
FROM sys.dm_db_task_space_usage dtsu
JOIN sys.dm_exec_sessions dess
ON dess.session_id=dtsu.session_id
GROUP BY dtsu.session_id, dess.login_name, dess.host_name, dess.login_time
ORDER BY NumOfPagesAllocatedInTempDBforInternalTask DESC, NumOfPagesAllocatedInTempDBforUserTask DESC

