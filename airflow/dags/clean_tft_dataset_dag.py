from airflow.decorators import dag, task                 # For using the @dag and @task decorators
from airflow.operators.python import get_current_context # For getting the current context in tasks
from datetime import datetime, timedelta, timezone       # For date and time handling
from pathlib import Path                                 # For file path handling
from sqlalchemy import create_engine                     # For database connection and queries
import logging                                           # For logging                      
import sys                                               # For modifying the system path
sys.path.insert(0, "/opt/airflow")

from src.config.load_config import load_config           # For loading config
from src.data.clean_dataset import (
    run_date_already_loaded,
    load_and_clean_data,
)                                                        # Importing functions to clean the dataset
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
user = config["DB"]["user"]
password = config["DB"]["password"]
database = config["DB"]["database"]
DB_URL = f"postgresql+psycopg2://{user}:{password}@tft-postgres:5432/{database}"

# --- DAG Configuration ---
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
}
# --- DAG Definition ---
@dag(
    dag_id="Clean_TFT_Dataset_DAG",
    schedule_interval = "0 0 * * 1,4", # Every Monday and Thursday at 00:00 UTC
    catchup=False,
    default_args=default_args,
    tags=["TFT", "Data-Pipeline"],
)
def clean_tft_dataset_pipeline():
    tz = timezone(timedelta(hours=7))
    # Task to load run dates
    # This task loads the run dates from the raw data directory and filters out already-loaded dates
    @task
    def task_load_run_dates():
        context = get_current_context()
        ds = context["ds"]
        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id

        start_time = datetime.now(tz)
        write_log(dag_id, task_id, ds, f"Started at {start_time.strftime('%Y-%m-%d, %H:%M:%S')}")

        engine = create_engine(DB_URL)
        try:
            with engine.connect() as conn:
                logger.info("Database connection successful.")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
        
        raw_data_root = Path("data/raw")
        run_dates = sorted([p.name for p in raw_data_root.iterdir() if p.is_dir()])
        filtered_run_dates = []
        for run_date in run_dates:
            if run_date_already_loaded(engine, run_date):
                logger.info(f"Skipping already-loaded run_date: {run_date}")
            else:
                filtered_run_dates.append(run_date)
        run_dates = filtered_run_dates
        
        end_time = datetime.now(tz)  
        duration = end_time - start_time
        mins, secs = divmod(duration.total_seconds(), 60)
        duration_str = f"{int(mins):02d}:{int(secs):02d}"
        
        write_log(dag_id, task_id, ds, f"Finished at {end_time.strftime('%Y-%m-%d, %H:%M:%S')} (Duration: {duration_str})")
        
        return run_dates
    # Task to load and clean data
    # This task loads and cleans the data for the given run dates
    @task
    def task_load_and_clean_data(run_dates):
        context = get_current_context()
        ds = context["ds"]
        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id

        start_time = datetime.now(tz)
        write_log(dag_id, task_id, ds, f"Started at {start_time.strftime('%Y-%m-%d, %H:%M:%S')}")
        write_log(dag_id, task_id, ds, f"Run dates to process: {len(run_dates)} date(s)")
        
        engine = create_engine(DB_URL)
        try:
            with engine.connect() as conn:
                logger.info("Database connection successful.")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
        
        for run_date in run_dates:
            logger.info(f"Processing run_date: {run_date}")
            inserted_counts = load_and_clean_data(engine, run_date)

            log_lines = [f"{table}: {count} new rows" for table, count in inserted_counts.items()]
            log_msg = f"Loaded and cleaned successfully for {run_date}\n" + "\n".join(log_lines)
            write_log(dag_id, task_id, ds, log_msg)
                
        end_time = datetime.now(tz)  
        duration = end_time - start_time
        mins, secs = divmod(duration.total_seconds(), 60)
        duration_str = f"{int(mins):02d}:{int(secs):02d}"
        
        write_log(dag_id, task_id, ds, f"Finished at {end_time.strftime('%Y-%m-%d, %H:%M:%S')} (Duration: {duration_str})")
                
    # Task dependencies
    run_dates = task_load_run_dates()
    task_load_and_clean_data(run_dates)

dag = clean_tft_dataset_pipeline()