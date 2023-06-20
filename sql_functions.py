from connection import conn
from psycopg import sql


def register_encode_var_as_json_and_base64():
	query = sql.SQL("""
		--- Encoding function
		CREATE OR REPLACE FUNCTION encode_var_as_json_and_base64(input_value anyelement)
		RETURNS text AS $$
		DECLARE
			json_text text := to_json(input_value);
			base64_text text := encode(json_text::bytea, 'base64');
		BEGIN
			RETURN base64_text;
		END;
		$$ LANGUAGE plpgsql;
		""")
	res = conn.execute(query)
	return res

def register_process_newly_added_row():
	query = sql.SQL("""
			--- Trigger callback
			CREATE OR REPLACE FUNCTION process_newly_added_row() RETURNS TRIGGER AS $$
			DECLARE
			  channel_name text := TG_ARGV[0];
			BEGIN
			  PERFORM pg_notify(channel_name, cron.encode_var_as_json_and_base64(NEW));
			  RETURN NEW;
			END;
			$$ LANGUAGE plpgsql;
			""")
	res = conn.execute(query)
	return res


def register_notify_trigger(channel_name, table_name, trigger_name):
	query = sql.SQL("""
				--- Trigger
				CREATE OR REPLACE TRIGGER {trigger_name}
				AFTER INSERT ON {table_name}
				FOR EACH ROW EXECUTE FUNCTION process_newly_added_row({channel_name});
				""")
	res = conn.execute(query.format(
		channel_name=channel_name,
		table_name=sql.Identifier(table_name),
		trigger_name=sql.Identifier(trigger_name)
	))
	return res

def schedule_notification(schedule_name, schedule_pattern, notification_channel, notification_message):
	query = sql.SQL(""" SELECT cron.schedule({schedule_name}, {schedule_pattern},  CONCAT('NOTIFY', ' ', {notification_channel}, ',', ' ', $${notification_message}$$) );  """)
	res = conn.execute(query.format(
		schedule_name=schedule_name,
		schedule_pattern=schedule_pattern,
		notification_channel=notification_channel,
		notification_message=notification_message
	))
	return res

def unschedule_task(schedule_name):
	query = sql.SQL("""SELECT cron.unschedule({schedule_name});""")
	res = conn.execute(query.format(
		schedule_name=schedule_name
	))
	return res


def create_job_run_details_trigger(table_name="job_run_details"):
	query = sql.SQL("""
		SELECT 
			jobid, 
			runid, 
			job_pid, 
			database, 
			username, 
			command, 
			status, 
			return_message, 
			start_time, 
			end_time 
		FROM {table_name};
	""")
	res = conn.execute(query.format(
		table_name=sql.Identifier(table_name)
	))
	print(res)
	return res
