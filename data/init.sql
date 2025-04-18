CREATE TABLE matches (
    match_id VARCHAR PRIMARY KEY,
    game_datetime TIMESTAMP,
    game_duration FLOAT,
    game_version VARCHAR,
    tft_set_number INT
);

CREATE TABLE participants (
    match_id VARCHAR REFERENCES matches(match_id),
    puuid VARCHAR,
    player_name VARCHAR,
    player_tagline VARCHAR,
    placement INT,
    level INT,
    total_damage_to_players INT,
    eliminated_player_count INT,
    time_eliminated FLOAT,
    last_round INT,
    gold_left INT,
    companion VARCHAR,
    PRIMARY KEY (match_id, puuid)
);

CREATE TABLE traits (
    match_id VARCHAR,
    puuid VARCHAR,
    trait_id VARCHAR,
    num_units INT,
    current_tier INT,
    max_tier INT,
    trait_style INT,
    PRIMARY KEY (match_id, puuid, trait_id),
    FOREIGN KEY (match_id, puuid) REFERENCES participants(match_id, puuid)
);

CREATE TABLE units (
    match_id VARCHAR,
    puuid VARCHAR,
    character_id VARCHAR,
    slot_idx INT,
    rarity INT,
    tier INT,
    items TEXT[],
    PRIMARY KEY (match_id, puuid, character_id, slot_idx),
    FOREIGN KEY (match_id, puuid) REFERENCES participants(match_id, puuid)
);

CREATE TABLE loaded_dates (
    loaded_date DATE PRIMARY KEY,
    last_loaded TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);