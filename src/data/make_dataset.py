from riotwatcher import TftWatcher, ApiError        # For API calls
from src.config.load_config import load_config      # For loading config
from datetime import datetime, timedelta, timezone  # For date and time handling
import pandas as pd                                 # For data manipulation
import time                                         # For timeouts
import random                                       # For random number generation
import logging                                      # For logging
  
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
api_key = config["riot"]["api_key"]

# --- Initialize Riot API watcher ---
tft_watcher = TftWatcher(api_key)

# --- Retry-safe API wrapper ---
def safe_api_call(func, *args, retries=3, **kwargs):
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except ApiError as err:
            status = err.response.status_code
            if status in [500, 503]:
                logger.warning(f"[{status}] Server error. Retrying in 5s... (Attempt {attempt+1}/{retries})")
                time.sleep(5)
            else:
                logger.error(f"[{status}] API error. Skipping. Details: {err}")
                return None
        except Exception as e:
            logger.error(f"[Error] {e}")
            return None
    return None
        
# --- Fetch players (challenger, grandmaster, master) ---
def fetch_summoner_puuids(platform_routing):
    logger.info("Fetching summoner PUUIDs...")
    logger.info(f"Platform Routing: {platform_routing}")
    
    # Initialize
    entries = []
    max_entries = 1000 
    
    for rank_func in [
        tft_watcher.league.challenger,
        tft_watcher.league.grandmaster,
    ]:
        league_data = safe_api_call(rank_func, region=platform_routing)
        if league_data:
            entries.extend(league_data["entries"])
            
    logger.info(f"Total players in Challenger and Grandmaster fetched: {len(entries)}")
    
    if len(entries) >= max_entries:
        logger.info(f"Max entries reached: {max_entries}. Stopping fetch.")
    else:
        logger.info("Fetching players from the Master tier to complete the list of 1000 players...")
        league_data = safe_api_call(tft_watcher.league.master, region=platform_routing)
        if league_data:
            space_left = max_entries - len(entries)
            entries.extend(league_data["entries"][:space_left])
        logger.info(f"Total players fetched: {len(entries)}")
    
    return [entry["puuid"] for entry in entries]

# --- Fetch yesterday's match IDs ---
def fetch_yesterday_match_ids(puuids, regional_routing):
    # Get yesterday's date
    utc_now = datetime.now(timezone.utc)
    yesterday = utc_now - timedelta(days=1)
    # Set 00:00:00 and 23:59:59 timestamps
    start_dt = datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0)
    end_dt = datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59)
    # Convert to epoch (seconds since Unix epoch)
    start_time = int(start_dt.timestamp())
    end_time = int(end_dt.timestamp())
    
    logger.info("Fetching match IDs...")
    logger.info(f"Regional Routing: {regional_routing}")
    logger.info(f"Total PUUIDs: {len(puuids)}")
    
    logger.info(f"Start: {start_dt} → Unix: {start_time}")
    logger.info(f"End  : {end_dt} → Unix: {end_time}")
    
    # Initialize
    all_match_ids = set()
    total_puuids = len(puuids)
    percent_step = 5
    last_logged_percent = -1
    
    for i, puuid in enumerate(puuids):
        match_ids = safe_api_call(
            tft_watcher.match.by_puuid,
            region=regional_routing,
            puuid=puuid,
            count=50,
            start_time=start_time,
            end_time=end_time
        )
        
        current_percent = int((i / total_puuids) * 100)
        if current_percent // percent_step > last_logged_percent // percent_step:
            logger.info(f"Fetched {i}/{total_puuids} PUUIDs ({current_percent}%)")
            last_logged_percent = current_percent
        
        time.sleep(1.2 + random.uniform(0.0, 0.05)) # Rate limit handling (100 requests per 120 seconds)
        
        if match_ids:
            all_match_ids.update(match_ids)
    
    logger.info("Fetching match IDs complete.")
    logger.info(f"Total match IDS found: {len(all_match_ids)}")
            
    return list(all_match_ids)

# --- Fetch full match data ---
def fetch_matches(match_ids, regional_routing):
    logger.info("Fetching match data...")
    logger.info(f"Regional Routing: {regional_routing}")
    logger.info(f"Total matches: {len(match_ids)}")
    
    # Initialize
    participants_data = []
    matches_info_data = []
    total_matches = len(match_ids)
    percent_step = 5
    last_logged_percent = -1
    
    for match_idx, match_id in enumerate(match_ids):
        match = safe_api_call(tft_watcher.match.by_id, region=regional_routing, match_id=match_id)
        
        current_percent = int((match_idx / total_matches) * 100)
        if current_percent // percent_step > last_logged_percent // percent_step:
            logger.info(f"Fetched {match_idx}/{total_matches} matches ({current_percent}%)")
            last_logged_percent = current_percent
        
        time.sleep(1.2 + random.uniform(0.0, 0.05))  # Rate limit handling (100 requests per 120 seconds)
        
        if not match:
            continue
        elif match['info']['queue_id'] != 1100:
            continue
        
        meta = match['metadata']
        info = match['info']
        
        match_info = {
            'match_id': meta['match_id'],
            'game_datetime': info['game_datetime'],
            'game_length': info['game_length'],
            'game_version': info['game_version'],
            'tft_set_number': info['tft_set_number'],
        }
        matches_info_data.append(match_info)
        
        for p in info['participants']:
            player = {
                'match_id': meta['match_id'],
                'puuid': p['puuid'],
                'name': p['riotIdGameName'],
                'tagline': p['riotIdTagline'],
                'placement': p['placement'],
                'level': p['level'],
                'traits': p['traits'],
                'units': p['units'],
                'total_damages': p['total_damage_to_players'],
                'player_eliminated': p['players_eliminated'],
                'time_eliminated': p['time_eliminated'],
                'last_round': p['last_round'],
                'gold_left': p['gold_left'],
                'companion': p['companion']['content_ID'],
            }
            participants_data.append(player)
    
    logger.info("Fetching match data complete.")
    logger.info(f"Total participants: {len(participants_data)}")
    logger.info(f"Total matches: {len(matches_info_data)}")
        
    return pd.DataFrame(participants_data), pd.DataFrame(matches_info_data)

# --- Main function for testing ---
def main():
    start_total = time.perf_counter()
    
    # Load config
    platform = config["riot"]["platform"]
    regional = config["riot"]["regional"]
    logger.info(f"Platform: {platform}")
    logger.info(f"Regional: {regional}")
    
    logger.info("[TEST] Fetching summoner PUUIDs...")
    start = time.perf_counter()
    puuids = fetch_summoner_puuids(platform_routing=platform)
    puuids = puuids[:50]  # Limit to 50 for testing
    logger.info(f"{len(puuids)} summoners fetched in {time.perf_counter() - start:.2f}s")

    logger.info("[TEST] Fetching match IDs...")
    start = time.perf_counter()
    match_ids = fetch_yesterday_match_ids(puuids, regional_routing=regional)
    logger.info(f"{len(match_ids)} matches found in {time.perf_counter() - start:.2f}s")

    logger.info("[TEST] Fetching matches data...")
    start = time.perf_counter()
    participants_df, matches_df = fetch_matches(match_ids, regional_routing=regional)
    logger.info(f"Match data shape: participants={participants_df.shape}, matches={matches_df.shape} "
          f"in {time.perf_counter() - start:.2f}s")

    logger.info(f"Test run complete — no CSVs were written. Total time: {time.perf_counter() - start_total:.2f}s")

if __name__ == "__main__":
    main()