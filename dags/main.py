from airflow import DAG  # pyright: ignore[reportMissingImports]
import pendulum  # pyright: ignore[reportMissingImports]
from datetime import datetime, timedelta

from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json
from datawarehouse.dwh import core_table, staging_table
from dataquality.soda import youtube_elt_data_quality

from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # pyright: ignore[reportMissingImports]

# Define the timezone
local_tz = pendulum.timezone("Asia/Manila")

# Default Args
default_args = {
    "owner" : "dataengineers",
    "depends_on_past" : False,
    "email_on_failure" : False,
    "email_on_retry" : False, 
    "email" : "data@engineers.com", 
    # "retries" : 1,
    # "retry_delay" : timedelta(minutes=5),
    "max_active_runs" : 1,
    "dagrun_timeout" : timedelta(hours=1),
    "start_date" : datetime(2026, 1, 1, tzinfo=local_tz),
    # "end_date" : datetime(2030, 12, 31, tzinfo=local_tz)
}

# Variables
staging_schema = "staging"
core_schema = "core"

# DAG 1: produce_json
with DAG(
    dag_id = 'produce_json',
    default_args = default_args,
    description = "DAG to produce JSON file with raw data",
    schedule = '0 14 * * *',
    catchup = False
) as dag_produce:

    # Define tasks
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_data)

    # Define trigger task for DAG update_db
    trigger_update_db = TriggerDagRunOperator(
        task_id = "trigger_update_db",
        trigger_dag_id = "update_db"
    )

    # Define dependecies
    playlist_id >> video_ids >> extract_data >> save_to_json_task >> trigger_update_db

# DAG 2: update_db
with DAG(
    dag_id = 'update_db',
    default_args = default_args,
    description = "DAG to process JSON file and insert data into both staging and core schemas",
    schedule = None, # Schedule is not set since this is being run by a trigger from produce_json DAG
    catchup = False
) as dag_update:

    # Define tasks
    update_staging = staging_table()
    update_core = core_table()

    # Define trigger task for DAG data quality
    trigger_data_quality = TriggerDagRunOperator(
        task_id = "trigger_data_quality",
        trigger_dag_id = "data_quality"
    )

    # Define dependecies
    update_staging >> update_core >> trigger_data_quality

# DAG 3: data_quality
with DAG(
    dag_id = 'data_quality',
    default_args = default_args,
    description = "DAG to check the data quality on both layers in the db",
    schedule = None, # Schedule is not set since this is being run by a trigger from update_db DAG
    catchup = False
) as dag_quality:

    # Define tasks
    soda_validate_staging = youtube_elt_data_quality(staging_schema)
    soda_validate_core = youtube_elt_data_quality(core_schema)

    # Define dependecies
    soda_validate_staging >> soda_validate_core