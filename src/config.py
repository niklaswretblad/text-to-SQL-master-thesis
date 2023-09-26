
import os
import yaml
from box import Box
import logging

# Enable logging
log_format = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(filename='debug.log', level=logging.DEBUG, format=log_format)

# Suppress debug logs from OpenAI and requests libraries
logging.getLogger("openai").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# Loading config.yaml file
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




