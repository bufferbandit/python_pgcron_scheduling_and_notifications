import base64
import json
import threading
import time

from connection import conn
from sql_functions import *
from pprint import pprint

def register_all():
    register_encode_var_as_json_and_base64()
    register_process_newly_added_row()
    register_notify_trigger(
        trigger_name="notify_newly_added_job_run_details_row",
        table_name="job_run_details",
        channel_name="watch_job_run_details"
    )

    register_notify_trigger(
        trigger_name="notify_newly_added_job_details_row",
        table_name="job",
        channel_name="watch_job_details"
    )





def watch_channel(channel_name=None,process_notification=lambda notify: None, stop_command="STOP_LISTENING", autostart_thread=True):
    def listen_thread():
        # If a channel name is specified
        if channel_name:
            # Loop through the channels
            # If channel is not a list make a list with the channel  name as the only element
            for channel in (channel_name if isinstance(channel_name, list) else [channel_name]):
                # Execute the listen command
                conn.execute("LISTEN " + channel)
        # Get the generator
        gen = conn.notifies()
        for notify in gen:
            # If the process_notification is a dict, use it to look up the callback
            if isinstance(process_notification, dict):
                if notify.channel in process_notification:
                    process_notification[notify.channel](notify)
                else:
                    process_notification["default"](notify)
            # Otherwise just call it as a callback
            else:
                process_notification(notify)
            # If the received command is stop, then stop
            if notify.payload == stop_command:
                gen.close()
                break

    # Create a new thread
    thread = threading.Thread(target=listen_thread)
    # Autostart it if specified
    if autostart_thread:
        thread.start()
    # Return the thread
    return thread


def process_added_row_notification(notify):
    data_b64 = notify.payload
    data_b64_bytes = base64.b64decode(data_b64)
    data_b64_str = data_b64_bytes.decode("utf-8")
    data = json.loads(data_b64_str)
    return data

def process_newly_started_run(notify):
    pprint(process_added_row_notification(notify))

def process_newly_started_job(notify):
    pprint(process_added_row_notification(notify))



if __name__ == "__main__":

    channel_name = "channel1"
    task_name = "notifying-task-1"
    register_all()
    try:
        # noinspection PyTypeChecker
        watch_channel(
                    [
                        channel_name,
                        "watch_job_run_details",
                        "watch_job_details"
                    ],
                    {
                        "watch_job_run_details":process_newly_started_run,
                        "watch_job_details":process_newly_started_job,
                        "channel1": lambda notify : None,
                        "default":lambda notify: print(notify)
                    }
        )
        res = schedule_notification(task_name, "3 seconds", channel_name, 'scheduling from python')


        time.sleep(30)
    finally:
        unschedule_task(task_name)
        pass





