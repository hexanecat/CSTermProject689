create or REPLACE function etl.log_etl_event (
	p_process_name text
	,p_target_table text
	,p_action_type text
	,p_rows_processed integer
	,p_status text
	,p_message text default null
	,p_source_file text default null
	)
returns void as $$

begin
	insert into etl.etl_log (
		process_name
		,target_table
		,action_type
		,rows_processed
		,status
		,message
		,source_file
		)
	values (
		p_process_name
		,p_target_table
		,p_action_type
		,p_rows_processed
		,p_status
		,p_message
		,p_source_file
		);
end;$$
