
import os
import yaml
from box import Box

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "../config/config.yaml")

def load_config():
    with open(CONFIG_PATH, 'r') as stream:
        try:
            config_data = yaml.safe_load(stream)
            return Box(config_data)
        except yaml.YAMLError as exc:
            print(exc)
            return None

config = load_config()

