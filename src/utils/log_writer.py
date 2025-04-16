from pathlib import Path
from datetime import datetime, timedelta, timezone

# --- Function to write logs to a file ---
def write_log(pipeline_stage: str, step_id: str, run_date: str, message: str):
    log_dir = Path(f"src/logs/{pipeline_stage}/{step_id}")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{run_date}.log"

    timestamp = datetime.now(timezone(timedelta(hours=7))).isoformat()
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")