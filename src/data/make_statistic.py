from sqlalchemy import create_engine, text                          # For database connection and queries
from mlxtend.frequent_patterns import fpgrowth                      # For frequent pattern mining
from mlxtend.preprocessing import TransactionEncoder                # For transaction encoding       
import pandas as pd                                                 # For data manipulation
import numpy as np                                                  # For numerical operations
import requests                                                     # For API calls
import re                                                           # For regular expressions

# --- Load configuration ---
from src.config.load_config import load_config                     
cfg = load_config()
DB_URL = f"postgresql+psycopg2://{cfg['DB']['user']}:{cfg['DB']['password']}@localhost:5431/{cfg['DB']['database']}"
engine = create_engine(DB_URL)

# --- Loading Functions ---
def load_matches():
    # --- Load matches data from database ---
    try:
        with engine.connect() as conn:
            matches_query = text("""
            SELECT 
                m.*, 
                split_part(m.game_version, '.', 1)::int AS game_major,
                split_part(m.game_version, '.', 2)::int AS game_minor
            FROM matches m
            ORDER BY game_major, game_minor, match_id
            """)
            matches_df = pd.read_sql(matches_query, conn)
    except Exception as e:
        raise Exception(f"Failed to load matches data: {e}")
    # --- Drop temporary version columns ---
    matches_df.drop(columns=['game_major', 'game_minor'], inplace=True)
    
    return matches_df

def load_participants():
    # --- Load participants data from database ---
    try:
        with engine.connect() as conn:
            participants_query = text("""
                SELECT 
                    m.game_version, m.match_id, p.puuid, p.placement, p.level,
                    split_part(m.game_version, '.', 1)::int AS game_major,
                    split_part(m.game_version, '.', 2)::int AS game_minor
                FROM participants p
                JOIN matches m ON p.match_id = m.match_id
                ORDER BY game_major, game_minor, match_id, placement
            """)
            participants_df = pd.read_sql(participants_query, conn)
    except Exception as e:
        raise Exception(f"Failed to load matches data: {e}")
    # --- Drop temporary version columns ---
    participants_df.drop(columns=['game_major', 'game_minor'], inplace=True)

    return participants_df

def load_units():
    try:
        with engine.connect() as conn:
            units_query = text("""
                SELECT
                    m.game_version, m.match_id, p.puuid,
                    u.character_id, u.slot_idx, u.rarity, u.tier, u.items
                FROM 
                    units u
                JOIN participants p ON u.match_id = p.match_id AND u.puuid = p.puuid
                JOIN matches m ON u.match_id = m.match_id
                WHERE u.character_id NOT IN (
                    'TFT14_SummonLevel2',
                    'TFT14_SummonLevel4',
                    'TFT14_Summon_Turret',
                    'TFT14_AnnieTibbers'
                )
            """)
            units_df = pd.read_sql(units_query, conn)
    except Exception as e:
        raise Exception(f"Failed to load matches data: {e}")

    # --- Reset slot index per player ---
    units_df['slot_idx'] = (
        units_df.sort_values(['match_id', 'puuid', 'slot_idx'])
        .groupby(['match_id', 'puuid'])
        .cumcount()
    )

    # --- Fast sort by version (split once, no columns created) ---
    version_split = units_df['game_version'].str.extract(r'(\d+)\.(\d+)').astype(int)
    units_df = units_df.assign(
        game_major=version_split[0],
        game_minor=version_split[1]
    ).sort_values(
        ['game_major', 'game_minor', 'match_id', 'puuid', 'slot_idx']
    ).drop(columns=['game_major', 'game_minor']).reset_index(drop=True)
    
    return units_df

def load_traits():
    try:
        with engine.connect() as conn:
            traits_query = text("""
                SELECT
                    m.game_version, m.match_id, p.puuid,
                    t.trait_id, t.num_units, t.current_tier, t.max_tier, t.trait_style,
                    split_part(m.game_version, '.', 1)::int AS game_major,
                    split_part(m.game_version, '.', 2)::int AS game_minor
                FROM 
                    traits t
                JOIN participants p ON t.match_id = p.match_id AND t.puuid = p.puuid
                JOIN matches m ON t.match_id = m.match_id
                ORDER BY game_major, game_minor, m.match_id, p.puuid, t.trait_id
            """)
            traits_df = pd.read_sql(traits_query, conn)
    except Exception as e:
        raise Exception(f"Failed to load matches data: {e}")
    # --- Drop temporary version columns ---
    traits_df.drop(columns=['game_major', 'game_minor'], inplace=True)

    return traits_df

def load_units_and_traits_info(units_df, traits_df):
    version = units_df['game_version'].unique()[0] + '.1'

    # --- Unit names ---
    url_unit = f'https://ddragon.leagueoflegends.com/cdn/{version}/data/en_US/tft-champion.json'
    unit_data = requests.get(url_unit).json()['data']
    unit_ids = units_df['character_id'].unique()
    
    # Map: unit_id -> unit_name
    unit_names = {unit['id']: unit["name"] for unit in unit_data.values() if unit['id'] in unit_ids}
    
    #  --- Trait names ---
    url_trait = f'https://ddragon.leagueoflegends.com/cdn/{version}/data/en_US/tft-trait.json'
    trait_data = requests.get(url_trait).json()['data']
    trait_ids = traits_df['trait_id'].unique()
    
    # Map: trait_id -> trait_name
    trait_names = {trait['id']: trait['name'] for trait in trait_data.values() if trait['id'] in trait_ids}
    # Map: trait_name -> trait_id
    trait_names_invert = {v: k for k, v in trait_names.items()}

    # --- Trait full info ===
    url = 'https://raw.communitydragon.org/latest/cdragon/tft/en_us.json'
    set_data = requests.get(url).json()['sets']
    traits_info = {}
    
    for set_info in set_data.values():
        for trait in set_info['traits']:
            trait_id = trait['apiName']
            if trait_id in trait_ids:
                # Map: trait_id -> {trait_name, tier -> min_units}
                traits_info[trait_id] = {
                    "name": trait_names.get(trait_id),
                    "tiers": {i + 1: effect['minUnits'] for i, effect in enumerate(trait['effects'])}
                }

    # --- Unit full info ---
    unit_traits_info = {}
    
    for set_info in set_data.values():
        for unit in set_info['champions']:
            if unit['apiName'] in unit_ids:
                # Map: unit_id -> {unit_name, [traits]}
                unit_traits_info[unit['apiName']] = {
                    "name": unit_names.get(unit['apiName']),
                    "traits": [trait_names_invert.get(trait) for trait in unit['traits']]
                }
    
    # --- Return ---
    return unit_traits_info, traits_info

def load_emblem_items_info(traits_df):
    version = traits_df['game_version'].unique()[0] + '.1'
    
    # --- Read the trait data from the API ---
    trait_url = f'https://ddragon.leagueoflegends.com/cdn/{version}/data/en_US/tft-trait.json'
    trait_data = requests.get(trait_url).json()['data']
    trait_ids = traits_df['trait_id'].unique()
    trait_names = {trait['name']: trait['id']  for trait in trait_data.values() if trait['id'] in trait_ids}
    
    # --- Read the item data from the API ---
    item_url = f'https://ddragon.leagueoflegends.com/cdn/{version}/data/en_US/tft-item.json'
    item_data = requests.get(item_url).json()['data']
    # Map: item_id -> image URL
    emblem_items_info = {item['id']: item['name'] for item in item_data.values() if 'Emblem' in item['id']}
    
    def extract_trait_from_emblem_value(value):
        match = re.match(r'(.+?)\s+Emblem$', value)
        if match:
            trait_name = match.group(1)
            return trait_name
        return None

    emblem_items_info = {
        k: trait_names.get(extract_trait_from_emblem_value(v))
        for k, v in emblem_items_info.items()
    }
    
    return emblem_items_info

# --- Trait & Unit Helpers ---
def sort_unit_list(unit_list, tier=False):
    def extract_sort_key(unit):
        try:
            # Format: TFTNumber_ChampionName_Rarity_(Tier)
            parts = unit.split('_')
            champion_name = parts[1]        # second = champion name
            rarity = int(parts[2])          # third = rarity
            if tier:
                unit_tier = int(parts[3])   # last = tier
                return (rarity, champion_name, unit_tier)
            else:
                return (rarity, champion_name)
        except:
            return (999, 'ZZZ', 999) if tier else (999, 'ZZZ')
    return sorted(unit_list, key=extract_sort_key)

def sort_trait_list(trait_list):
    def extract_sort_key(trait):
        try:
            # Format: TFTNumber_TraitName_NumUnit_Tier
            parts = trait.split('_')
            trait_name = parts[1]           # second = champion name
            num_unit = int(parts[2])        # third = num unit
            trait_tier = int(parts[3])      # last = tier
            return (-num_unit, -trait_tier, trait_name)
        except:
            return (999, 999, 'ZZZ')
    return sorted(trait_list, key=extract_sort_key)

# --- Composition Comparators Helpers ---
def get_units_tier(units):
    # Format: [TFTNumber_ChampionName_Rarity_Tier] -> [Tier]
    return [int(unit.split("_")[-1]) for unit in units]
def get_units_without_tier(units):
    # Format: [TFTNumber_ChampionName_Rarity_Tier] -> [TFTNumber_ChampionName_Rarity]
    return [unit.rsplit('_', 1)[0] for unit in units]
# Normalize unit tiers for only tier 2 or higher units
def normalize_unit_tiers(full_units):
    normalized = []
    for unit in full_units:
        parts = unit.split('_')
        if len(parts) == 4 and parts[-1] == '1':
            parts[-1] = '2'
        normalized.append('_'.join(parts))
    return tuple(sort_unit_list(normalized, tier=True))
# Deduplicate compositions in a DataFrame by removing subsets of itemsets in upper levels of the same game version
def deduplicate_comps(frequent_comps_df):
    """
    Deduplicates compositions in a DataFrame by removing subsets of itemsets 
    within the same game version.

    This function processes a DataFrame containing compositions and their 
    associated game versions, levels, and itemsets. For each game version, 
    it identifies and retains only the unique compositions where no itemset 
    is a subset of another within the same game version.

    Args:
        frequent_comps_df (pd.DataFrame): A DataFrame containing the following columns:
            - 'game_version': The version of the game.
            - 'itemsets': A list or iterable representing the items in the composition.
            - 'level': The level associated with the composition.

    Returns:
        pd.DataFrame: A DataFrame containing the deduplicated compositions with the following columns:
            - 'game_version': The version of the game.
            - 'itemsets': The unique itemsets for the game version.
            - 'level': The level associated with the unique composition.
    """
    result = []
    for game_version in frequent_comps_df['game_version'].unique():
        game_version_df = frequent_comps_df[frequent_comps_df['game_version'] == game_version]
        # Convert to (tuple, set) for efficient comparison
        comps = [(row['itemsets'], set(row['itemsets']), row['level']) 
                 for _, row in game_version_df.iterrows()]
        unique_comps = []
        for comp_tuple, comp_set, level in comps:
            is_unique = True
            for _, unique_set, _ in unique_comps:
                if unique_set.issubset(comp_set):
                    is_unique = False
                    break
            if is_unique:
                unique_comps.append((comp_tuple, comp_set, level))
        for comp_tuple, _, level in unique_comps:
            result.append({
                'game_version': game_version,
                'itemsets': comp_tuple,
                'level': level,
            })
    return pd.DataFrame(result)
# Find active traits of a combination of units
def load_combine_units_traits(combine_units, unit_traits_info, traits_info):
    trait_counter = {}
    for unit in combine_units:
        unit = '_'.join(unit.split('_')[:2])
        unit_info = unit_traits_info.get(unit)
        traits = unit_info.get('traits', [])
        for trait in traits:
            trait_counter[trait] = trait_counter.get(trait, 0) + 1
    
    traits_to_remove = []
    for trait, count in trait_counter.items():
        trait_info = traits_info.get(trait)
        trait_tier = trait_info.get('tiers')
        first_tier_count = list(trait_tier.values())[0]
        
        if count < first_tier_count:
            traits_to_remove.append(trait)
    
    for trait in traits_to_remove:
        del trait_counter[trait]
            
    return trait_counter
# Find the most common compositions from level to level 9 of a given composition
def find_most_common_comps_by_level(comps_df, comp, level):
    base_comp = set(comp)
    prev_comp = None
    results = []

    for lvl in range(level, 10):
        filtered_df = comps_df[comps_df['level'] == lvl].copy()
        mask = filtered_df['combine_units'].apply(lambda x: base_comp.issubset(set(x)))
        matched = filtered_df[mask]
        if matched.empty:
            results.append({'level': lvl, 'comp': None})
            continue
        most_common_full_units = (
            matched.groupby('combine_full_units')
            .size()
            .reset_index(name='count')
            .sort_values(by='count', ascending=False)
        )
        normalized_comp = None
        for _, row in most_common_full_units.iterrows():
            candidate = normalize_unit_tiers(row['combine_full_units'])
            candidate_base = set(get_units_without_tier(candidate))
            if prev_comp is None or prev_comp.issubset(candidate_base):
                normalized_comp = candidate
                prev_comp = candidate_base
                break
         
        results.append({
            'level': lvl,
            'comp': normalized_comp
        })
    return results

# --- Legality Checkers ---
def is_legal_trait_count(count, trait_tiers):
    thresholds = sorted(trait_tiers.values())
    if count < thresholds[0]:
        return False
    return count in thresholds  # Must exactly match one of the tier thresholds
def is_legal_comp(unit_traits_info, traits_info, unit_list=None, trait_list=None, ignore_threshold=1):
    """
    Determines whether a given composition of units or traits is legal based on the provided traits information.

    Args:
        unit_traits_info (dict): A dictionary containing unit-to-traits mapping. 
            Example format: { "TFTNumber_ChampionName": {"traits": ["TFTNumber_TraitName", ...]} }.
        traits_info (dict): A dictionary containing trait information, including tiers and thresholds.
            Example format: { "TFTNumber_TraitName": {"tiers": {tier_level: required_count, ...}} }.
        unit_list (list[tuple], optional): A list of unit identifiers in the format "TFTNumber_ChampionName_Tier_Rarity".
            If provided, the function will calculate trait counts based on this list.
        trait_list (list[tuple], optional): A list of trait identifiers in the format "TFTNumber_TraitName_NumUnits_Tier".
            If `unit_list` is not provided, the function will calculate trait counts based on this list.
        ignore_threshold (int, optional): The maximum number of illegal traits allowed before the composition is considered illegal. Defaults to 1.

    Raises:
        ValueError: If `unit_traits_info` or `traits_info` is not provided.
        ValueError: If neither `unit_list` nor `trait_list` is provided.

    Returns:
        bool: True if the composition is legal, False otherwise.

    Notes:
        - A trait is considered illegal if the number of units contributing to it does not meet the thresholds defined in `traits_info`.
        - If the number of illegal traits exceeds `ignore_threshold`, the composition is deemed illegal.
    """
    trait_counter = {}
    if unit_traits_info is None or traits_info is None:
        raise ValueError("unit_traits_info and traits_info must be provided.") 
    # --- Use unit_list if provided ---
    if unit_list is not None:
        for unit in unit_list:
            # Format: TFTNumber_ChampionName_Tier_Rarity
            unit_parts = unit.split('_')
            unit_id = f"{unit_parts[0]}_{unit_parts[1]}"
            traits_ids = unit_traits_info.get(unit_id, {}).get('traits', [])
            for trait_id in traits_ids:
                trait_counter[trait_id] = trait_counter.get(trait_id, 0) + 1
    # --- Use trait_list if unit_list is not provided ---
    elif trait_list is not None:
        for trait in trait_list:
            # Format: TFTNumber_TraitName_NumUnits_Tier
            trait_parts = trait.split('_')
            trait_id = f"{trait_parts[0]}_{trait_parts[1]}"
            trait_counter[trait_id] = int(trait_parts[2])  
    # --- If neither unit_list nor trait_list is provided, raise Error ---
    else:
        raise ValueError("Either unit_list or trait_list must be provided.")
    # --- Check legality ---
    exception_count = 0
    for trait_id, count in trait_counter.items():
        if count < 2:
            continue
        tiers = traits_info.get(trait_id, {}).get('tiers', {})
        if not is_legal_trait_count(count, tiers):
            exception_count += 1
            if exception_count > ignore_threshold:
                return False
    return True

# --- Statistic Trait & Unit DataFrames Builder ---
def build_units_statistic_df(participants_df, units_df, basic=False):
    """
    Builds a DataFrame containing statistical information about units in a game.
    This function processes data about game participants and their units to compute
    various statistics, including average placement, frequency, win rate, and top-4 rate.
    Optionally, it can also generate statistics for item builds associated with units.
    Args:
        participants_df (pd.DataFrame): A DataFrame containing participant data with the following columns:
            - 'game_version': The version of the game.
            - 'match_id': The unique identifier for the match.
            - 'puuid': The unique identifier for the player.
            - 'placement': The placement of the player in the match.
        units_df (pd.DataFrame): A DataFrame containing unit data with the following columns:
            - 'game_version': The version of the game.
            - 'match_id': The unique identifier for the match.
            - 'puuid': The unique identifier for the player.
            - 'character_id': The identifier for the unit.
            - 'rarity': The rarity of the unit.
            - 'items': A list of items equipped by the unit.
        basic (bool, optional): If True, returns a simplified DataFrame with only basic unit statistics.
            Defaults to False.
    Returns:
        pd.DataFrame: A DataFrame containing unit statistics. The structure of the DataFrame depends on the value of `basic`:
            - If `basic` is True:
                Columns include:
                - 'game_version': The version of the game.
                - 'character_id': The identifier for the unit.
                - 'rarity': The rarity of the unit.
                - 'frequency': The frequency of the unit's appearance (as a percentage).
                - 'avg_place': The average placement of the unit.
                - 'win': The win rate of the unit.
                - 'top4': The top-4 rate of the unit.
            - If `basic` is False:
                Columns include:
                - 'game_version': The version of the game.
                - 'character_id': The identifier for the unit.
                - 'rarity': The rarity of the unit.
                - 'item_build': The item build associated with the unit.
                - 'frequency': The frequency of the item build's appearance (as a percentage of the unit's overall frequency).
                - 'avg_place': The average placement of the unit with the item build.
                - 'avg_delta': The difference between the average placement of the unit with the item build and the unit's overall average placement.
                - 'win': The win rate of the unit with the item build.
                - 'top4': The top-4 rate of the unit with the item build.
    Notes:
        - Units with invalid rarity (e.g., -1) are excluded from the analysis.
        - Item builds with a frequency below 10 are filtered out.
        - The function sorts the final DataFrame by game version, character ID, and frequency for clean output.
    """
    # --- Join with participants to get placement info ---
    units_df = units_df.merge(
        participants_df[['game_version', 'match_id', 'puuid', 'placement']], 
        on=['game_version', 'match_id', 'puuid'], how='left'
    )
    
    # --- Remove invalid units (e.g., NoneUnit) ---
    units_df = units_df[units_df['rarity'] > -1].copy()
    total_comps = (units_df.groupby('game_version')['match_id'].nunique() * 8).to_dict()
    
    # --- Compute overall unit average placement and frequency ---
    unit_avg_placement = (
        units_df
        .groupby(['game_version', 'character_id', 'rarity'], as_index=False)
        .agg(
            avg_place=('placement', 'mean'),
            frequency=('placement', 'count'),
            win=('placement', lambda x: (x == 1).mean()),
            top4=('placement', lambda x: (x <= 4).mean())
        )
        .assign(
            avg_place=lambda df: df['avg_place'].round(2),
            win=lambda df: df['win'].round(3),
            top4=lambda df: df['top4'].round(3),
        )
    )
    unit_avg_placement['frequency_percent'] = unit_avg_placement.apply(
        lambda row: round(row['frequency'] / (total_comps[row['game_version']]), 3),
        axis=1
    )
    
    if basic:
        unit_avg_placement = unit_avg_placement.drop(columns=['frequency'])
        unit_avg_placement = unit_avg_placement.rename(columns={'frequency_percent': 'frequency'})
        
        # Finalize column order
        unit_avg_placement = unit_avg_placement[[
            'game_version', 'character_id', 'rarity', 
             'frequency', 'avg_place', 'win', 'top4'
        ]]
        
        # Finalize row order
        # Split game_version into major/minor parts for correct numerical sorting
        unit_avg_placement[['game_major', 'game_minor']] = (
            unit_avg_placement['game_version'].str.split('.', expand=True).astype(int)
        )

        # Sort rows by game version, then rarity, then character_id
        unit_avg_placement = (
            unit_avg_placement.sort_values(
                by=['game_major', 'game_minor', 'rarity', 'character_id'],
                ascending=[True, True, True, False]
            )
            .drop(columns=['game_major', 'game_minor'])
            .reset_index(drop=True)
        )
        
        return unit_avg_placement

    # --- Generate item builds ---
    gloves_items = ['ThiefsGloves', 'TFT9_Item_OrnnPrototypeForge']
    
    def clean_item_build(items):
        if not isinstance(items, list) or len(items) == 0:
            return ''
        
        gloves_only = [item for item in items if any(gloves_item in item for gloves_item in gloves_items)]
        if len(gloves_only) == 1:
            return gloves_only[0]

        return '+'.join(sorted(items))
    
    item_builds_stats_df = units_df.copy()
    item_builds_stats_df['item_build'] = item_builds_stats_df['items'].apply(clean_item_build)
    item_builds_stats_df = item_builds_stats_df[item_builds_stats_df['item_build'] != '']

    # --- Group by build and calculate stats ---
    item_builds_stats_df = (
        item_builds_stats_df
        .groupby(['game_version', 'character_id', 'rarity', 'item_build'], as_index=False)
        .agg(
            frequency=('placement', 'count'),
            avg_place=('placement', 'mean'),
            win=('placement', lambda x: (x == 1).mean()),
            top4=('placement', lambda x: (x <= 4).mean())
        )
    )

    # --- Filter out builds with low frequency ---
    item_builds_stats_df = item_builds_stats_df[item_builds_stats_df['frequency'] >= 10].copy()

    # --- Merge with unit overall stats ---
    item_builds_stats_df = (
        item_builds_stats_df
        .merge(
            unit_avg_placement[['game_version', 'character_id', 'rarity', 'avg_place', 'frequency']],
            on=['game_version', 'character_id', 'rarity'],
            suffixes=('', '_overall'),
            how='inner'
        )
        .assign(
            frequency=lambda df: (df['frequency'] / df['frequency_overall']).round(3),
            avg_delta=lambda df: (df['avg_place'] - df['avg_place_overall']).round(2),
            avg_place=lambda df: df['avg_place'].round(2),
            win=lambda df: df['win'].round(3),
            top4=lambda df: df['top4'].round(3)
        )
        .drop(columns=['avg_place_overall', 'frequency_overall'])
    )

    # --- Finalize ---
    units_stats_df = item_builds_stats_df[item_builds_stats_df['frequency'] > 0].copy()
    
    # --- Finalize column order ---
    # Reorder columns for clean output
    units_stats_df = units_stats_df[[ 
        'game_version', 'character_id', 'rarity', 'item_build',
        'frequency', 'avg_place', 'avg_delta', 'win', 'top4'
    ]]

    # --- Finalize row order ---
    # Split game_version into major/minor parts for correct numerical sorting
    units_stats_df[['game_major', 'game_minor']] = (
        units_stats_df['game_version'].str.split('.', expand=True).astype(int)
    )

    # Sort rows by game version, then rarity, then character_id
    units_stats_df = (
        units_stats_df.sort_values(
            by=['game_major', 'game_minor', 'rarity', 'character_id'],
            ascending=[True, True, True, False]
        )
        .drop(columns=['game_major', 'game_minor'])
        .reset_index(drop=True)
    )

    return units_stats_df

def build_traits_statistic_df(participants_df, traits_df, traits_info):
    """
    Build a DataFrame containing statistics for traits based on game data.
    This function processes and aggregates data related to traits in a game, 
    calculating various performance metrics such as play rate, average placement, 
    win rate, and top-4 rate. It also includes additional feature engineering 
    and ensures the output is sorted and formatted for analysis.
    Args:
        participants_df (pd.DataFrame): DataFrame containing participant information, 
            including columns:
            - 'game_version': Version of the game.
            - 'match_id': Unique identifier for the match.
            - 'puuid': Unique identifier for the player.
            - 'placement': Placement of the player in the match.
        traits_df (pd.DataFrame): DataFrame containing trait information, 
            including columns:
            - 'game_version': Version of the game.
            - 'match_id': Unique identifier for the match.
            - 'puuid': Unique identifier for the player.
            - 'trait_id': Identifier for the trait.
            - 'current_tier': Current tier of the trait.
            - 'max_tier': Maximum tier of the trait.
            - 'trait_style': Style or category of the trait.
        traits_info (dict): Dictionary containing metadata about traits, 
            where keys are trait IDs and values are dictionaries with the following structure:
            {
                'tiers': {
                    <tier>: <min_units_required>,
                    ...
    Returns:
        pd.DataFrame: A DataFrame containing aggregated statistics for traits, 
        with the following columns:
        - 'game_version': Version of the game.
        - 'trait_id': Identifier for the trait.
        - 'current_tier': Current tier of the trait.
        - 'max_tier': Maximum tier of the trait.
        - 'current_tier_log': Logarithmic feature derived from current_tier / max_tier.
        - 'trait_style': Style or category of the trait.
        - 'min_units_required': Minimum units required for the trait at the current tier.
        - 'play_rate': Proportion of matches where the trait was used.
        - 'avg_place': Average placement of players using the trait.
        - 'win': Proportion of matches where the trait contributed to a win.
        - 'top4': Proportion of matches where the trait contributed to a top-4 placement.
    Notes:
        - Invalid traits (e.g., traits with current_tier or max_tier <= 0) are excluded.
        - The output is sorted by game version, trait ID, and current tier.
        - The function assumes that 'game_version' follows a "major.minor" format 
          for proper numerical sorting.
    """
    # --- Join with participants to get placement info ---
    traits_df = traits_df.merge(
        participants_df[['game_version', 'match_id', 'puuid', 'placement']], 
        on=['game_version', 'match_id', 'puuid'], how='left'
    )
    
    # --- Remove invalid traits (e.g., NoneTrait) ---
    traits_df = traits_df[(traits_df['current_tier'] > 0) & (traits_df['max_tier'] > 0)] .copy()
    
    # --- Precompute (trait_id, tier) -> min_units_required mapping ---
    trait_min_units_map = {
        (trait_id, tier): min_unit
        for trait_id, info in traits_info.items()
        for tier, min_unit in info.get('tiers', {}).items()
    }

    # --- Prepare min_units_required per trait ---
    trait_min_units = (
        traits_df[['game_version', 'trait_id', 'current_tier', 'max_tier']]
        .drop_duplicates()
        .assign(
            min_units_required=lambda df: df.apply(
                lambda row: trait_min_units_map.get((row['trait_id'], row['current_tier']), 0),
                axis=1
            )
        )
    )

    # --- Set group keys ---
    group_keys = ['game_version', 'trait_id', 'current_tier', 'trait_style']
    total_matches = traits_df.groupby('game_version')['match_id'].nunique().to_dict()

    # --- Precompute win and top4 flags ---
    traits_df = traits_df.assign(
        win=(traits_df['placement'] == 1).astype(int),
        top4=(traits_df['placement'] <= 4).astype(int)
    )

    # --- Compute play rate and aggregate performance ---
    trait_stats = (
        traits_df
        .groupby(group_keys, as_index=False)
        .agg(
            play_count=('match_id', 'size'),
            avg_place=('placement', 'mean'),
            win=('win', 'mean'),
            top4=('top4', 'mean')
        )
    )

    # --- Round statistics ---
    trait_stats['play_rate'] = trait_stats.apply(
        lambda row: round(row['play_count'] / total_matches[row['game_version']], 2),
        axis=1
    )
    trait_stats['avg_place'] = trait_stats['avg_place'].round(2)
    trait_stats['win'] = trait_stats['win'].round(3)
    trait_stats['top4'] = trait_stats['top4'].round(3)

    # --- Merge with min_units_required ---
    traits_stats_df = trait_stats.merge(trait_min_units, on=['game_version', 'trait_id', 'current_tier'], how='left')
    traits_stats_df.drop(columns=['play_count'], inplace=True)

    # --- Finalize column order ---
    # Reorder columns for clean output
    traits_stats_df = traits_stats_df[[
        'game_version', 'trait_id', 'current_tier', 'max_tier', 'trait_style',
        'min_units_required', 'play_rate', 'avg_place', 'win', 'top4'
    ]]
    
    # --- Finalize row order ---
    # Split game_version into major/minor parts for correct numerical sorting
    traits_stats_df[['game_major', 'game_minor']] = (
        traits_stats_df['game_version'].str.split('.', expand=True).astype(int)
    )
    # Sort rows by game version, then trait_id, then current_tier
    traits_stats_df = (
        traits_stats_df.sort_values(
            by=['game_major', 'game_minor', 'trait_id', 'current_tier'],
            ascending=[True, True, True, True]
        )
        .drop(columns=['game_major', 'game_minor'])
        .reset_index(drop=True)
    )

    return traits_stats_df

# --- Statistic Item DataFrames Builder ---
def build_items_statistic_df(participants_df, units_df):
    """
    Builds a DataFrame containing statistical information about items in games.
    This function computes various statistics for items, including their frequency, play rate, average placement,
    win rate, top-4 rate, and the top units associated with each item. It also classifies items into different types
    (e.g., Craftable, Radiant, Artifact, etc.) and organizes the output for clean presentation.
    Parameters:
    -----------
    participants_df : pd.DataFrame
        A DataFrame containing participant-level data, including game version, match ID, player unique ID (puuid),
        and placement information.
    units_df : pd.DataFrame
        A DataFrame containing unit-level data, including game version, match ID, player unique ID (puuid),
        character IDs, and items equipped by units.
    Returns:
    --------
    pd.DataFrame
        A DataFrame containing item statistics with the following columns:
        - 'game_version': The version of the game.
        - 'items': The item identifier.
        - 'frequency': The normalized frequency of the item across participants.
        - 'play_rate': The normalized play rate of the item across matches.
        - 'avg_place': The average placement of participants using the item.
        - 'win': The win rate (percentage of first-place finishes) for the item.
        - 'top4': The top-4 rate (percentage of top-4 finishes) for the item.
        - 'top_units': A list of the top units associated with the item, including their frequency, average placement,
          and other statistics.
        - 'type': The classified type of the item (e.g., Craftable, Radiant, Artifact, etc.).
    Notes:
    ------
    - Items with low frequency (less than 1%) are filtered out from unit-level statistics.
    - Component items and empty bags are excluded from the final output.
    - The output is sorted by game version, item type, and item name for better readability.
    - Custom item type classifications are defined for specific sets of items.
    """
    # --- Basic unit stats ---
    unit_basic_stats = build_units_statistic_df(participants_df, units_df, basic=True)
    
    # --- Join with participants to get placement info ---
    units_df = units_df.merge(
        participants_df[['game_version', 'match_id', 'puuid', 'placement']],
        on=['game_version', 'match_id', 'puuid'], how='left'
    )
    
    # --- Only keep relevant columns ---
    units_df = units_df[units_df['items'].map(lambda x: isinstance(x, list) and len(x) > 0)]
    units_df_exploded = units_df.explode('items').reset_index(drop=True)

    # --- Compute total matches per game version ---
    total_matches = units_df.groupby('game_version')['match_id'].nunique().to_dict()    
    # --- Compute total participant ---
    total_participants = {k: v * 8 for k, v in total_matches.items()}
    
    # --- Compute item stats ---
    items_stats_df = (
        units_df_exploded
        .groupby(['game_version', 'items'])
        .agg(
            frequency=('items', 'count'),
            avg_place=('placement', 'mean'),
            win=('placement', lambda x: (x == 1).mean()),
            top4=('placement', lambda x: (x <= 4).mean())
        )
        .reset_index()
    )

    items_stats_df['play_rate'] = items_stats_df.apply(
        lambda row: round(row['frequency'] / total_matches[row['game_version']], 2),
        axis=1
    )
    items_stats_df['frequency'] = items_stats_df.apply(
        lambda row: round(row['frequency'] / total_participants[row['game_version']], 3),
        axis=1
    )
    items_stats_df['avg_place'] = items_stats_df['avg_place'].round(2)
    items_stats_df['win'] = items_stats_df['win'].round(3)
    items_stats_df['top4'] = items_stats_df['top4'].round(3)

    # --- Compute item units frequency ---
    item_total = (
        units_df_exploded
        .groupby(['game_version', 'items'])
        .size()
        .reset_index(name='total_count')
    )

    # --- Compute per-unit stats --
    unit_stats = (
        units_df_exploded
        .groupby(['game_version', 'items', 'character_id'])
        .agg(
            count=('placement', 'count'),
            avg_place=('placement', 'mean'),
            win=('placement', lambda x: (x == 1).mean()),
            top4=('placement', lambda x: (x <= 4).mean())
        )
        .reset_index()
    )

    # --- Merge total counts and compute frequency ---
    unit_stats = (
        unit_stats
        .merge(item_total, on=['game_version', 'items'])
        .merge(unit_basic_stats[['game_version', 'character_id', 'avg_place']], on=['game_version', 'character_id'], how='left', suffixes=('', '_unit'))
        .merge(items_stats_df[['game_version', 'items', 'avg_place']], on=['game_version', 'items'], how='left', suffixes=('', '_item'))
    )
    unit_stats['frequency'] = unit_stats['count'] / unit_stats['total_count']
    unit_stats['item_avg_delta'] = unit_stats['avg_place'] - unit_stats['avg_place_item']
    unit_stats['unit_avg_delta'] = unit_stats['avg_place'] - unit_stats['avg_place_unit']
    
    # --- Filter out low frequency units ---
    unit_stats = unit_stats[unit_stats['frequency'] >= 0.01].copy()

    # --- Format top units per item ---
    rows = []
    for (game_version, item), group in unit_stats.groupby(['game_version', 'items']):
        group_sorted = group.sort_values(by='frequency', ascending=False).head(10)
        top_units = [
            {'character_id': row['character_id'], 'frequency': round(row['frequency'], 3),
             'avg_place': round(row['avg_place'], 2), 
             'item_avg_delta': round(row['item_avg_delta'], 2), 'unit_avg_delta': round(row['unit_avg_delta'], 2),
             'win': round(row['win'], 3), 'top4': round(row['top4'], 3)}
            for _, row in group_sorted.iterrows()
        ]
        rows.append({
            'game_version': game_version,
            'items': item,
            'top_units': top_units
        })
    top_units_df = pd.DataFrame(rows)

    # --- Merge with top_units ---
    items_stats_df = items_stats_df.merge(top_units_df, on=['game_version', 'items'], how='left')
    items_stats_df['top_units'] = items_stats_df['top_units'].apply(lambda x: x if isinstance(x, list) else None)

    # --- Define item type sets ---
    COMPONENT_ITEMS = {
        'TFT_Item_BFSword', 'TFT_Item_RecurveBow', 'TFT_Item_NeedlesslyLargeRod', 'TFT_Item_TearOfTheGoddess',
        'TFT_Item_ChainVest', 'TFT_Item_NegatronCloak', 'TFT_Item_GiantsBelt', 'TFT_Item_SparringGloves',
        'TFT_Item_Spatula', 'TFT_Item_FryingPan'
    }
    RADIANT_ITEMS = {
        'TFT5_Item_AdaptiveHelmRadiant', 'TFT5_Item_ArchangelsStaffRadiant', 'TFT5_Item_BloodthirsterRadiant', 'TFT5_Item_BlueBuffRadiant',
        'TFT5_Item_BrambleVestRadiant', 'TFT5_Item_CrownguardRadiant', 'TFT5_Item_DeathbladeRadiant', 'TFT5_Item_DragonsClawRadiant',
        'TFT5_Item_FrozenHeartRadiant', 'TFT5_Item_GargoyleStoneplateRadiant', 'TFT5_Item_GiantSlayerRadiant', 'TFT5_Item_GuardianAngelRadiant',
        'TFT5_Item_GuinsoosRagebladeRadiant', 'TFT5_Item_HandOfJusticeRadiant', 'TFT5_Item_HextechGunbladeRadiant', 'TFT5_Item_InfinityEdgeRadiant',
        'TFT5_Item_IonicSparkRadiant', 'TFT5_Item_JeweledGauntletRadiant', 'TFT5_Item_LastWhisperRadiant', 'TFT5_Item_LeviathanRadiant',
        'TFT5_Item_MorellonomiconRadiant', 'TFT5_Item_NightHarvesterRadiant', 'TFT5_Item_QuicksilverRadiant', 'TFT5_Item_RabadonsDeathcapRadiant',
        'TFT5_Item_RapidFirecannonRadiant', 'TFT5_Item_RedemptionRadiant', 'TFT5_Item_RunaansHurricaneRadiant', 'TFT5_Item_SpearOfShojinRadiant',
        'TFT5_Item_SpectralGauntletRadiant', 'TFT5_Item_StatikkShivRadiant', 'TFT5_Item_SteraksGageRadiant', 'TFT5_Item_SunfireCapeRadiant',
        'TFT5_Item_ThiefsGlovesRadiant', 'TFT5_Item_TitansResolveRadiant', 'TFT5_Item_TrapClawRadiant', 'TFT5_Item_WarmogsArmorRadiant'
    }
    ARTIFACT_ITEMS = {
        'TFT4_Item_OrnnAnimaVisage', 'TFT9_Item_OrnnPrototypeForge', 'TFT_Item_Artifact_BlightingJewel', 'TFT4_Item_OrnnDeathsDefiance',
        'TFT9_Item_OrnnDeathfireGrasp', 'TFT_Item_Artifact_Fishbones', 'TFT7_Item_ShimmerscaleGamblersBlade', 'TFT4_Item_OrnnTheCollector',
        'TFT_Item_Artifact_HorizonFocus', 'TFT9_Item_OrnnHullbreaker', 'TFT4_Item_OrnnInfinityForce', 'TFT_Item_Artifact_InnervatingLocket',
        'TFT_Item_Artifact_LichBane', 'TFT_Item_Artifact_LightshieldCrest', 'TFT_Item_Artifact_LudensTempest', 'TFT4_Item_OrnnMuramana',
        'TFT_Item_Artifact_Mittens', 'TFT7_Item_ShimmerscaleMogulsMail', 'TFT_Item_Artifact_ProwlersClaw', 'TFT_Item_Artifact_RapidFirecannon',
        'TFT_Item_Artifact_SeekersArmguard', 'TFT_Item_Artifact_SilvermereDawn', 'TFT9_Item_OrnnHorizonFocus', 'TFT_Item_Artifact_SpectralCutlass',
        'TFT_Item_Artifact_SuspiciousTrenchCoat', 'TFT_Item_Artifact_TalismanOfAscension', 'TFT9_Item_OrnnTrickstersGlass',
        'TFT_Item_Artifact_UnendingDespair', 'TFT_Item_Artifact_WitsEnd', 'TFT4_Item_OrnnZhonyasParadox'
    }
    SUPPORT_ITEMS = {
        'TFT_Item_AegisOfTheLegion', 'TFT_Item_BansheesVeil', 'TFT_Item_Chalice', 'TFT_Item_SupportKnightsVow',
        'TFT_Item_LocketOfTheIronSolari', 'TFT_Item_Moonstone', 'TFT7_Item_ShimmerscaleHeartOfGold', 'TFT4_Item_OrnnObsidianCleaver',
        'TFT4_Item_OrnnRanduinsSanctum', 'TFT_Item_Shroud', 'TFT_Item_Spite', 'TFT_Item_EternalFlame',
        'TFT_Item_UnstableTreasureChest', 'TFT_Item_RadiantVirtue', 'TFT_Item_ZekesHerald', 'TFT_Item_Zephyr',
        'TFT5_Item_ZzRotPortalRadiant'
    }
    EXOTECH_ITEMS = {
        'TFT14_JaxCyberneticItem', 'TFT14_JhinCyberneticItem', 'TFT14_MordekaiserCyberneticItem',
        'TFT14_NaafiriCyberneticItem', 'TFT14_SejuaniCyberneticItem', 
        'TFT14_VarusCyberneticItem', 'TFT14_ZeriCyberneticItem'
    }
    EXOTECH_RADIANT_ITEMS = {
        'TFT14_JaxCyberneticItem_Radiant', 'TFT14_JhinCyberneticItem_Radiant', 'TFT14_MordekaiserCyberneticItem_Radiant',
        'TFT14_NaafiriCyberneticItem_Radiant', 'TFT14_SejuaniCyberneticItem_Radiant', 
        'TFT14_VarusCyberneticItem_Radiant', 'TFT14_ZeriCyberneticItem_Radiant'
    }

    # --- Classify item types ---
    def filter_items_type(item):
        if item in 'TFT_Item_EmptyBag':
            return 'EmptyBag'
        elif item in COMPONENT_ITEMS:
            return 'Component'
        elif item in RADIANT_ITEMS:
            return 'Radiant'
        elif item in ARTIFACT_ITEMS:
            return 'Artifact'
        elif item in SUPPORT_ITEMS:
            return 'Support'
        elif item in EXOTECH_ITEMS:
            return 'ExotechItem'
        elif item in EXOTECH_RADIANT_ITEMS:
            return 'ExotechRadiantItem'
        elif 'Emblem' in item:
            return 'Emblem'
        else:
            return 'Craftable'

    items_stats_df['type'] = items_stats_df['items'].map(filter_items_type)
    items_stats_df = items_stats_df[
        (items_stats_df['type'] != 'Component') & 
        (items_stats_df['type'] != 'EmptyBag')
    ].reset_index(drop=True)
    
    # --- Finalize column order ---
    # Reorder columns for clean output
    items_stats_df = items_stats_df[[
        'game_version', 'items', 'frequency', 'play_rate', 'avg_place', 
        'win', 'top4', 'top_units', 'type'
    ]]
    
    # --- Finalize row order ---
    # Split game_version into major/minor parts for correct numerical sorting
    items_stats_df[['game_major', 'game_minor']] = (
        items_stats_df['game_version'].str.split('.', expand=True).astype(int)
    )
    
    # Define custom type order
    type_order = ['Craftable', 'Emblem', 'Radiant', 'Artifact', 'Support', 'ExotechItem']
    type_order_map = {name: i for i, name in enumerate(type_order)}
    items_stats_df['type_rank'] = items_stats_df['type'].map(type_order_map)

    # Sort rows by game version, then type_rank, then items
    items_stats_df = (
        items_stats_df.sort_values(
            by=['game_major', 'game_minor', 'type_rank', 'items'],
            ascending=[True, True, True, True]
        )
        .drop(columns=['game_major', 'game_minor', 'type_rank'])
        .reset_index(drop=True)
    )
    
    return items_stats_df

# --- Comps DataFrame Builder ---
def build_comps_df(participants_df, units_df, traits_df):
    """
    Builds a DataFrame containing detailed information about team compositions 
    in a match, including units, traits, levels, and placements.
    Args:
        participants_df (pd.DataFrame): DataFrame containing participant-level information 
            such as game version, match ID, player unique identifier (puuid), level, and placement.
        units_df (pd.DataFrame): DataFrame containing unit-level information such as rarity, 
            tier, items, and associated traits.
        traits_df (pd.DataFrame): DataFrame containing trait-level information such as 
            trait ID, number of units contributing to the trait, and current tier.
    Returns:
        pd.DataFrame: A DataFrame containing aggregated and processed information about 
        team compositions with the following columns:
            - game_version (str): The version of the game.
            - match_id (str): The unique identifier for the match.
            - puuid (str): The unique identifier for the player.
            - placement (int): The player's placement in the match.
            - level (int): The player's level in the match.
            - combine_units (tuple): A sorted tuple of unit combinations (character_id and rarity).
            - combine_full_units (tuple): A sorted tuple of full unit combinations (character_id, rarity, and tier).
            - combine_active_traits (tuple): A sorted tuple of active trait combinations (trait_id, num_units, and current_tier).
            - combine_full_traits (tuple): A sorted tuple of all trait combinations (trait_id, num_units, and current_tier).
            - has_emblem (bool): Whether the player has any emblem items.
            - emblems (list): A list of traits associated with emblem items.
    Notes:
        - Units with invalid rarity (-1) are filtered out.
        - Traits with max_tier <= 0 are excluded from processing.
        - The resulting DataFrame is filtered to include only valid compositions where:
            1. The number of units matches the player's level.
            2. There are non-empty active and full trait combinations.
        - The final DataFrame is sorted by game version, match ID, and placement.
    """
    # --- Prepare unit combinations ---
    units_filtered = units_df[units_df['rarity'] > -1].copy()
    # Combine_unit = character_id_rarity
    units_filtered['combine_unit'] = (units_filtered['character_id'] + '_' + units_filtered['rarity'].astype(str))
    # Combine_full_unit = character_id_rarity_tier
    units_filtered['combine_full_unit'] = (units_filtered['combine_unit'] + '_' + units_filtered['tier'].astype(str))

    # Handle items safely
    emblem_items_info = load_emblem_items_info(traits_df)
    units_filtered['items'] = units_filtered['items'].apply(lambda x: x if isinstance(x, list) else [])
    units_filtered['emblem_items'] = units_filtered['items'].apply(
        lambda items: [item for item in items if 'Emblem' in item]
    )
    units_filtered['has_emblem'] = units_filtered['emblem_items'].apply(lambda x: len(x) > 0)
    units_filtered['emblems'] = units_filtered['emblem_items'].apply(
        lambda items: [emblem_items_info.get(item) for item in items]
    )

    # --- Aggregate unit information ---
    units_agg = (
        units_filtered
        .groupby(['game_version', 'match_id', 'puuid'], as_index=False)
        .agg(
            combine_units=('combine_unit', list),
            combine_full_units=('combine_full_unit', list),
            has_emblem=('has_emblem', 'any'),
            emblems=('emblems', lambda x: [trait for sub in x if isinstance(sub, list) for trait in sub])
        )
    )

    # Clean & sort unit lists
    units_agg['combine_units'] = units_agg['combine_units'].apply(lambda x: x if isinstance(x, list) else [])
    units_agg['combine_units'] = units_agg['combine_units'].apply(lambda x: tuple(sort_unit_list(x)))
    units_agg['combine_full_units'] = units_agg['combine_full_units'].apply(lambda x: x if isinstance(x, list) else [])
    units_agg['combine_full_units'] = units_agg['combine_full_units'].apply(lambda x: tuple(sort_unit_list(x, tier=True)))

    # --- Prepare full trait combinations ---
    traits_filtered = traits_df[traits_df['max_tier'] > 0].copy()
    # Combine_trait = trait_id_num_units_current_tier
    traits_filtered['combine_trait'] = (traits_filtered['trait_id'] + '_' + traits_filtered['num_units'].astype(str) + '_' + traits_filtered['current_tier'].astype(str))
    # ALl traits (current_tier >= 0)
    traits_agg_full = (
        traits_filtered
        .groupby(['game_version', 'match_id', 'puuid'], as_index=False)
        .agg(
            combine_full_traits=('combine_trait', list),
        )
    )
    # Active traits (current_tier > 0)
    active_traits_filtered = traits_filtered[traits_filtered['current_tier'] > 0].copy()
    active_traits_filtered = active_traits_filtered[['game_version', 'match_id', 'puuid', 'combine_trait']]
    traits_agg_active = (
        active_traits_filtered
        .groupby(['game_version', 'match_id', 'puuid'], as_index=False)
        .agg(
            combine_active_traits=('combine_trait', list)
        )
    )
    # Merge active + full
    traits_agg = traits_agg_full.merge(traits_agg_active, on=['game_version', 'match_id', 'puuid'], how='inner')
    traits_agg['combine_active_traits'] = traits_agg['combine_active_traits'].apply(lambda x: tuple(sort_trait_list(x)))
    traits_agg['combine_full_traits'] = traits_agg['combine_full_traits'].apply(lambda x: tuple(sort_trait_list(x)))

    # --- Prepare level info ---
    level_df = (
        participants_df[['game_version', 'match_id', 'puuid', 'level', 'placement']]
        .drop_duplicates()
    )
    
    # --- Merge and finalize ---
    comps_df = (
        units_agg
        .merge(traits_agg, on=['game_version', 'match_id', 'puuid'], how='inner')
        .merge(level_df, on=['game_version', 'match_id', 'puuid'], how='left')
    )
    # Filter out entries with empty units or traits
    comps_df = comps_df[
        (comps_df['combine_units'].apply(len) > 0) & 
        (comps_df['combine_active_traits'].apply(len) > 0) & 
        (comps_df['combine_full_traits'].apply(len) > 0)
    ]
    # Keep only valid comps: unit count == level
    comps_df['num_units'] = comps_df['combine_units'].apply(len)
    comps_df = comps_df[comps_df['num_units'] == comps_df['level']].reset_index(drop=True)
    comps_df.drop(columns=['num_units'], inplace=True)
    # Keep legal comp by function is_legal_comp
    unit_traits_info, traits_info = load_units_and_traits_info(units_df, traits_df)
    comps_df = comps_df[
        comps_df.apply(
            lambda row: is_legal_comp(
                trait_list=row['combine_active_traits'],
                unit_traits_info=unit_traits_info, traits_info=traits_info
            ), axis=1
        )
    ]
    
    # --- Finalize column order ---
    comps_df = comps_df[[
        'game_version', 'match_id', 'puuid', 'placement', 'level',
        'combine_units', 'combine_full_units', 'combine_active_traits', 'combine_full_traits', 
        'has_emblem', 'emblems'
    ]]

    # --- Finalize row order ---
    # Split game_version into major/minor parts for correct numerical sorting
    comps_df[['game_major', 'game_minor']] = (
        comps_df['game_version'].str.split('.', expand=True).astype(int)
    )
    
    # Sort rows by game version, then match_id, then placement
    comps_df = (
        comps_df.sort_values(by=['game_major', 'game_minor', 'match_id', 'placement'])
        .drop(columns=['game_major', 'game_minor'])
        .reset_index(drop=True)
    )
    
    return comps_df

def build_frequent_comps_by_version(comps_by_version, min_support=1e-3, top=10, unit_traits_info=None, traits_info=None):
    """
    Generate frequent compositions by game version and level using the FP-Growth algorithm.
    Args:
        comps_by_version (dict): A dictionary where keys are tuples of (game_version, level) 
            and values are lists of compositions (transactions) for each version and level.
        min_support (float, optional): The minimum support threshold for the FP-Growth algorithm. 
            Defaults to 1e-3.
        top (int, optional): The maximum number of top frequent compositions to return for each 
            game version and level. Defaults to 10.
        unit_traits_info (dict, optional): Information about unit traits. Must be provided. 
            Defaults to None.
        traits_info (dict, optional): Additional traits information. Must be provided. 
            Defaults to None.
    Raises:
        ValueError: If `unit_traits_info` or `traits_info` is not provided.
    Returns:
        pd.DataFrame: A concatenated DataFrame containing the top frequent compositions for 
        each game version and level. The DataFrame includes the following columns:
            - 'game_version': The game version.
            - 'level': The level of the game.
            - 'itemsets': The frequent itemsets (compositions).
            - 'support': The support value of the itemsets, rounded to 4 decimal places.
    """
    if unit_traits_info is None or traits_info is None:
        raise ValueError("unit_traits_info and traits_info must be provided.")
    
    results = []

    for (game_version, level), comps in comps_by_version.items():
        te = TransactionEncoder()
        trans = te.fit_transform(comps)
        trans_df = pd.DataFrame(trans, columns=te.columns_)

        freq = fpgrowth(trans_df, min_support=min_support, use_colnames=True)
        freq['length'] = freq['itemsets'].apply(len)
        comps = freq[freq['length'] == level].copy()
        comps['itemsets'] = comps['itemsets'].apply(lambda x: tuple(sort_unit_list(x)))

        comps = (
            comps.sort_values(by='support', ascending=False)
            .head(top)
            .reset_index(drop=True)
        )
        comps['game_version'] = game_version
        comps['level'] = level
        comps['support'] = (comps['support']).round(4)
        results.append(comps[['game_version', 'level', 'itemsets', 'support']])

    return pd.concat(results, ignore_index=True)

# --- Comps Statistic DataFrame Builder ---
def build_comps_statistic_df(comps_df, freq_df, unit_traits_info, traits_info):
    """
    Builds a DataFrame containing statistical information about team compositions 
    in a game based on the provided data.
    Args:
        comps_df (pd.DataFrame): DataFrame containing information about compositions 
            and their placements in matches. Expected columns include 'game_version', 
            'match_id', 'combine_units', and 'placement'.
        freq_df (pd.DataFrame): DataFrame containing frequency data for itemsets 
            and their associated levels. Expected columns include 'game_version', 
            'itemsets', and 'level'.
        unit_traits_info (dict): Dictionary containing information about unit traits.
        traits_info (dict): Dictionary containing information about traits.
    Returns:
        pd.DataFrame: A DataFrame where each row represents a statistical summary 
        for a specific game version and itemset. The DataFrame includes the following columns:
            - 'game_version': The version of the game.
            - 'level 7': The most common composition at level 7.
            - 'level 7 traits': Traits associated with the level 7 composition.
            - 'level 8': The most common composition at level 8.
            - 'level 8 traits': Traits associated with the level 8 composition.
            - 'level 9': The most common composition at level 9.
            - 'level 9 traits': Traits associated with the level 9 composition.
            - 'play_count': The number of matches where the itemset was played.
            - 'play_rate': The rate at which the itemset was played, relative to 
              the total matches in the game version.
            - 'avg_place': The average placement of the itemset in matches.
            - 'win': The win rate of the itemset (proportion of matches where 
              the itemset placed first).
            - 'top4': The top-4 rate of the itemset (proportion of matches where 
              the itemset placed in the top 4).
    """
    total_matches = comps_df.groupby('game_version')['match_id'].nunique().to_dict()
    records = []

    for _, row in freq_df.iterrows():
        version = row['game_version']
        itemset = set(row['itemsets'])
        df_filtered = comps_df[comps_df['game_version'] == version]
        mask = df_filtered['combine_units'].apply(lambda x: itemset.issubset(set(x)))
        matched = df_filtered[mask]
        
        if not matched.empty:
            play_count = len(matched)
            
            avg_place = matched['placement'].mean()
            win = (matched['placement'] == 1).sum()
            top4 = (matched['placement'] <= 4).sum()
            combine_units_by_level = find_most_common_comps_by_level(comps_df, itemset, level=row['level'])

            play_rate = round(play_count / total_matches[version], 2)
            win_rate = round(win / play_count, 3)
            top4_rate = round(top4 / play_count, 3)

            # Extract level 7 composition and traits
            level_7_comp = next((entry['comp'] for entry in combine_units_by_level if entry['level'] == 7), None)
            level_7_traits = load_combine_units_traits(level_7_comp, unit_traits_info, traits_info) if level_7_comp is not None else None
            # Extract level 8 composition and traits
            level_8_comp = next((entry['comp'] for entry in combine_units_by_level if entry['level'] == 8), None)
            level_8_traits = load_combine_units_traits(level_8_comp, unit_traits_info, traits_info) if level_8_comp else None
            # Extract level 9 composition and traits
            level_9_comp = next((entry['comp'] for entry in combine_units_by_level if entry['level'] == 9), None)
            level_9_traits = load_combine_units_traits(level_9_comp, unit_traits_info, traits_info) if level_9_comp else None

            records.append({
                'game_version': version,
                'level_7': level_7_comp,
                'level_7_traits': level_7_traits,
                'level_8': level_8_comp,
                'level_8_traits': level_8_traits,
                'level_9': level_9_comp,
                'level_9_traits': level_9_traits,
                'play_rate': play_rate,
                'avg_place': avg_place,
                'win': win_rate,
                'top4': top4_rate
            })
    
    return pd.DataFrame(records)

# --- Main function for testing ---
def main():
    units_df = load_units()
    traits_df = load_traits()
    if units_df is None or traits_df is None:
        print("ERROR: Failed to load units and traits data.")
        return

    unit_traits_info, traits_info = load_units_and_traits_info(units_df, traits_df)
    comps_df = build_comps_df(units_df)
    
    print("Data fetched and processed successfully.")
    print(f"\nExample unit_traits_info: {list(unit_traits_info.items())[:5]}")
    print(f"\nExample traits_info: {list(traits_info.items())[:5]}")
    print(f"\nExample comps_df: {comps_df[['character_combine', 'has_emblem']].head()}")
    print(f"\nExample comps_df has emblem: {comps_df[comps_df['has_emblem'] == True][['character_combine', 'has_emblem']].head()}")
    
if __name__ == "__main__":
    main()