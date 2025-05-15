import pandas as pd
import logging
from datetime import datetime
import ast
import os
from sqlalchemy import create_engine, text

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

# --- Run Date Check ---
def run_date_already_loaded(engine, run_date):
    try:
        with engine.connect() as conn:
            logger.info(f"Checking if run_date {run_date} is already loaded...")
            result = conn.execute(
                text("SELECT COUNT(*) FROM loaded_dates WHERE loaded_date = :run_date"),
                {"run_date": run_date}
            ).scalar()
        return result > 0
    except Exception as e:
        logger.error(f"Database check failed for {run_date}: {e}", exc_info=True)
        return False
    
# --- Load and Clean Data ---
def load_and_clean_data(engine, run_date):
    try:
        base_dir = f"data/raw/{run_date}"
        participants_path = os.path.join(base_dir, "participants.csv")
        matches_path = os.path.join(base_dir, "matches.csv")

        # --- Load CSV files ---
        participants_df = pd.read_csv(participants_path)
        matches_df = pd.read_csv(matches_path)

        # --- Traits ---
        participants_df['traits'] = participants_df['traits'].apply(ast.literal_eval)
        traits_df = participants_df[['match_id', 'puuid', 'traits']].copy()
        # Filling empty traits in player comp
        no_trait = [{
            'name': 'TFT_NoneTrait', 'num_units': -1, 
            'style': -1, 'tier_current': -1, 'tier_total': -1
        }]
        traits_df['traits'] = traits_df['traits'].apply(
            lambda x: x if isinstance(x, list) and len(x) > 0 else no_trait
        )
        # Exploding the traits column to separate rows for each trait
        traits_df = traits_df.explode('traits', ignore_index=True)
        traits_df = traits_df.join(pd.json_normalize(traits_df.pop('traits')))
        # Renaming columns for same naming in SQL table
        traits_df.rename(columns={
            'name': 'trait_id', 'tier_current': 'current_tier', 'tier_total': 'max_tier',
            'style': 'trait_style'
        }, inplace=True)

        # --- Units ---
        participants_df['units'] = participants_df['units'].apply(ast.literal_eval)
        units_df = participants_df[['match_id', 'puuid', 'units']].copy()
        # Filling emty units in player comp
        no_unit = [{
            'character_id': 'TFT_NoneUnit', 'itemNames': [],
            'name': '', 'rarity': -1, 'tier': -1
        }]
        units_df['units'] = units_df['units'].apply(
            lambda x: x if isinstance(x, list) and len(x) > 0 else no_unit
        )        
        # Adding slot_index to each unit in the list
        # This is important for the case of 2 units with same name in the same comp
        units_df['units'] = units_df['units'].apply(
            lambda units: [
                {**unit, 'slot_idx': idx} for idx, unit in enumerate(units)
            ]
        )
        # Explode the units column to separate rows for each unit
        units_df = units_df.explode('units', ignore_index=True)
        units_df = units_df.join(pd.json_normalize(units_df.pop('units')))  
        # Drop unnecessary name column as it is a empty column
        units_df = units_df.drop(columns=['name'], errors='ignore')
        # Rename columns for same naming in SQL table
        units_df.rename(columns={'itemNames': 'items'}, inplace=True)
        # Remove unit after Viego
        def remove_unit_after_viego(df):
            summon_units = {
                'TFT14_SummonLevel2',    # R-080T: Summoned by Nitro trait units (e.g., Elise, Shyvana). Gains Chrome over rounds.
                'TFT14_SummonLevel4',    # T-43X: Upgraded form of R-080T when 200 Chrome is accumulated.
                'TFT14_Summon_Turret',   # Nitro Hex Turret: Summoned from augments related to the Nitro trait.
                'TFT14_AnnieTibbers'         # Tibbers: Summoned by Annie after every 4 casts of her ability
            }

            viego_rows = df[df['character_id'] == 'TFT14_Viego'].sort_values('slot_idx')
            if viego_rows.empty:
                return df.copy()

            last_viego = viego_rows.iloc[-1]
            last_viego_slot = last_viego['slot_idx']
            idx = last_viego_slot + 1
            unit_removed = False

            while True:
                next_unit = df[df['slot_idx'] == idx]
                if next_unit.empty:
                    break
                next_char = next_unit.iloc[0]['character_id']
                if next_char in summon_units:
                    idx += 1
                    continue
                else:
                    df = df.drop(index=next_unit.index[0])
                    unit_removed = True
                    break

            # Only check if there are 2 Viego or more and did not removed any unit
            if len(viego_rows) >= 2 and not unit_removed:
                viego_items = last_viego.get('items', [])
                if isinstance(viego_items, list) and len(viego_items) == 1:
                    df = df.drop(index=last_viego.name)

            df = df.sort_values('slot_idx').reset_index(drop=True)
            df['slot_idx'] = df.index
            return df
        units_df = (
            units_df.sort_values(['match_id', 'puuid', 'slot_idx'])
            .groupby(['match_id', 'puuid'], group_keys=False)
            .apply(remove_unit_after_viego)
            .reset_index(drop=True)
        )
        
        # --- Matches ---
        matches_df = matches_df[['match_id', 'game_datetime', 
                                 'game_length', 'game_version', 'tft_set_number']].copy()
        # Converting game_datetime from milliseconds to datetime format (for SQL table timestamp)
        matches_df['game_datetime'] = pd.to_datetime(matches_df['game_datetime'], unit='ms')
        # Extracting the date part from the game version (e.g., 15.7.672.4034 -> 15.7)
        matches_df['game_version'] = matches_df['game_version'].str.extract(r'(\d+\.\d+)\.\d+\.\d+')
        # Renaming columns for same naming in SQL table
        matches_df.rename(columns={'game_length': 'game_duration'}, inplace=True)

        # --- Participants ---
        participants_df = participants_df[['match_id', 'puuid', 'name', 'tagline', 'placement', 'level',
                                           'total_damages', 'player_eliminated', 'time_eliminated',
                                           'last_round', 'gold_left', 'companion']].copy()
        # Renaming columns for same naming in SQL table
        participants_df.rename(columns={
            'name': 'player_name', 'tagline': 'player_tagline',
            'total_damages': 'total_damage_to_players', 'player_eliminated': 'eliminated_player_count',
        }, inplace=True)

        # --- Upload all to database in a single transaction ---
        matches_df.to_sql("matches", engine, if_exists="append", index=False)
        participants_df.to_sql("participants", engine, if_exists="append", index=False)
        traits_df.to_sql("traits", engine, if_exists="append", index=False)
        units_df.to_sql("units", engine, if_exists="append", index=False)
        
        # Insert the run_date into loaded_dates table
        # This table is used to track which run_dates have been loaded
        with engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO loaded_dates (loaded_date, last_loaded)
                    VALUES (:run_date, :last_loaded)
                    ON CONFLICT (loaded_date) DO NOTHING
                """),
                {"run_date": run_date, "last_loaded": datetime.now()}
            )

        logger.info(f"Data loaded successfully into the database for run_date: {run_date}.")
        logger.info(f"Matches DataFrame shape: {matches_df.shape}")
        logger.info(f"Participants DataFrame shape: {participants_df.shape}")
        logger.info(f"Traits DataFrame shape: {traits_df.shape}")
        logger.info(f"Units DataFrame shape: {units_df.shape}")
        
        return {
            "matches": len(matches_df),
            "participants": len(participants_df),
            "units": len(units_df),
            "traits": len(traits_df),
        }

    except Exception as e:
        logger.error(f"Error during processing run_date={run_date}: {e}", exc_info=True)
        raise
    
# --- Main function for testing ---
def main():
    DB_URL = "postgresql+psycopg2://postgres:pass@localhost:5431/tft_data"
    engine = create_engine(DB_URL)
    try:
        with engine.connect() as conn:
            logger.info("Database connection successful.")

            # Check if the table exists
            query = text("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'loaded_dates';
            """)
            result = conn.execute(query).fetchall()
            for column in result:
                logger.info(f"{column}")
            
            # Check if the loaded_dates table exists
            run_dates = ["2025-04-14", "2025-04-15", "2025-04-16"]
            for run_date in run_dates:
                if not run_date_already_loaded(engine, run_date):
                    logger.info(f"{run_date} not loaded yet.")
                else:
                    logger.info(f"{run_date} already loaded.")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
    
if __name__ == "__main__":
    main()