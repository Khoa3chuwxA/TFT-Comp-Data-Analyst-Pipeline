import os
import yaml

# --- Load YAML config file ---
def load_config(path=None):
    """
    Load YAML config from the provided path, or use default in /config/
    """
    if path is None:
        base_dir = os.path.dirname(__file__)
        path = os.path.join(base_dir, "config.yaml")

    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found at: {path}")

    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
    
if __name__ == "__main__":
    cfg = load_config()
    print("Riot API Key:", cfg["riot"]["api_key"])
    print("Platform Region:", cfg["riot"]["platform_region"])
    print("Regional Routing:", cfg["riot"]["regional_region"])
    print("Database Name:", cfg["DB"]["user"])
    print("Database User:", cfg["DB"]["password"])
    print("Database Password:", cfg["DB"]["database"])