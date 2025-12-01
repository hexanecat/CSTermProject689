create
	or REPLACE function truncate_staging_tables ()
returns void language plpgsql as $$

declare stmt text;

begin
	select string_agg('TRUNCATE TABLE ' || quote_ident(table_schema) || '.' || quote_ident(table_name) || ' RESTART IDENTITY CASCADE;', ' ')
	into stmt
	from information_schema.tables
	where table_schema = 'staging'
		and table_type = 'BASE TABLE';

	if stmt is not null THEN
		execute stmt;
end

if ;end;$$;