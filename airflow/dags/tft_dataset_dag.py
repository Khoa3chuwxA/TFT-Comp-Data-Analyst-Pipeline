from airflow.decorators import dag, task                 # For using the @dag and @task decorators
from airflow.operators.python import get_current_context # For getting the current context in tasks
from datetime import datetime, timedelta, timezone       # For date and time handling
from pathlib import Path                                 # For file path handling
import logging                                           # For logging                      
import sys                                               # For modifying the system path
sys.path.insert(0, "/opt/airflow")

from src.data.make_dataset import (
    fetch_summoner_puuids,
    fetch_yesterday_match_ids,
    fetch_matches,
)                                                        # Importing functions to fetch data from the API
from src.config.load_config import load_config           # For loading config
from src.utils.log_writer import write_log               # Importing the function to write logs

# --- Logging Setup ---
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    fmt='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d, %H:%M:%S'
)

handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)

# --- Load Config ---
config = load_config()
platform = config["riot"]["platform"]
regional = config["riot"]["regional"]

# --- DAG Configuration ---
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
# --- DAG Definition ---
@dag(
    dag_id="TFT_Dataset_DAG",
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["TFT", "Data-Pipeline"],
)
def tft_dataset_pipeline():
    tz = timezone(timedelta(hours=7))
    # Task to fetch summoner PUUIDs
    # This task fetches the PUUIDs of players in the Challenger, Grandmaster, and Master tiers
    @task
    def task_fetch_summoner_puuids():
        context = get_current_context()
        ds = context["ds"]
        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id
        
        start_time = datetime.now(tz)
        write_log(dag_id, task_id, ds, f"Started at {start_time.strftime('%Y-%m-%d, %H:%M:%S')}")

        # Fetch summoner PUUIDs
        puuids = fetch_summoner_puuids(platform_routing=platform)
        
        end_time = datetime.now(tz)
        duration = end_time - start_time
        mins, secs = divmod(duration.total_seconds(), 60)
        duration_str = f"{int(mins):02d}:{int(secs):02d}"
        
        write_log(dag_id, task_id, ds, f"Fetched {len(puuids)} summoner PUUIDs")
        write_log(dag_id, task_id, ds, f"Finished at {end_time.strftime('%Y-%m-%d, %H:%M:%S')} (Duration: {duration_str})")
        
        return puuids
    # Task to fetch match IDs for the given PUUIDs
    # This task fetches match IDs for the previous day based on the PUUIDs obtained from the previous task
    @task
    def task_fetch_yesterday_match_ids(puuids):
        context = get_current_context()
        ds = context["ds"]
        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id
        
        start_time = datetime.now(tz)
        write_log(dag_id, task_id, ds, f"Started at {start_time.strftime('%Y-%m-%d, %H:%M:%S')}")
        
        # Fetch match IDs for the given PUUIDs
        match_ids = fetch_yesterday_match_ids(puuids, regional_routing=regional)
        
        end_time = datetime.now(tz)
        duration = end_time - start_time
        mins, secs = divmod(duration.total_seconds(), 60)
        duration_str = f"{int(mins):02d}:{int(secs):02d}"
        
        write_log(dag_id, task_id, ds, f"Fetched {len(match_ids)} match IDs")
        write_log(dag_id, task_id, ds, f"Finished at {end_time.strftime('%Y-%m-%d, %H:%M:%S')} (Duration: {duration_str})")

        return match_ids
    # Task to fetch and save match data
    # This task fetches match data for the given match IDs and saves it to CSV files
    @task
    def task_fetch_and_save_matches(match_ids):
        context = get_current_context()
        ds = context["ds"]
        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id
        
        start_time = datetime.now(tz)
        write_log(dag_id, task_id, ds, f"Started at {start_time.strftime('%Y-%m-%d, %H:%M:%S')}")
        
        # Fetch match data for the given match IDs
        participants_df, matches_df = fetch_matches(match_ids, regional_routing=regional)

        participants_df.sort_values(by=["match_id", "placement"], inplace=True)
        participants_df.reset_index(drop=True, inplace=True)

        matches_df.sort_values(by=["match_id", "game_datetime"], inplace=True)
        matches_df.reset_index(drop=True, inplace=True)
        
        output_dir = Path(f"data/raw/{ds}")
        output_dir.mkdir(parents=True, exist_ok=True)

        participants_df.to_csv(output_dir / "participants.csv", index=False)
        matches_df.to_csv(output_dir / "matches.csv", index=False)

        end_time = datetime.now(tz)
        duration = end_time - start_time
        mins, secs = divmod(duration.total_seconds(), 60)
        duration_str = f"{int(mins):02d}:{int(secs):02d}"
        
        write_log(dag_id, task_id, ds, f"Saved {len(participants_df)} participants records")
        write_log(dag_id, task_id, ds, f"Saved {len(matches_df)} match records")
        write_log(dag_id, task_id, ds, f"Finished at {end_time.strftime('%Y-%m-%d, %H:%M:%S')} (Duration: {duration_str})")

    # Task dependencies
    # The tasks are executed in the following order:
    puuids = task_fetch_summoner_puuids()
    match_ids = task_fetch_yesterday_match_ids(puuids)
    task_fetch_and_save_matches(match_ids)

dag = tft_dataset_pipeline()