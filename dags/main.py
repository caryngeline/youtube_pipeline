from airflow import DAG  # pyright: ignore[reportMissingImports]
import pendulum  # pyright: ignore[reportMissingImports]
from datetime import datetime, timedelta

try:
    from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json
except ModuleNotFoundError:
    # Fallback when dags/ is not on path (e.g. some Airflow setups)
    import sys
    from pathlib import Path
    _dags_dir = Path(__file__).resolve().parent
    if str(_dags_dir) not in sys.path:
        sys.path.insert(0, str(_dags_dir))
    from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json

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

with DAG(
    dag_id = 'produce_json',
    default_args = default_args,
    description = "DAG to produce JSON file with raw data",
    schedule = '0 14 * * *',
    catchup = False
) as dag:

    # Define tasks
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_data)

    # Define dependecies
    playlist_id >> video_ids >> extract_data >> save_to_json_task