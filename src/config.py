
import os
import yaml
from box import Box
import logging
import wandb


CONFIG_BASE_PATH = os.path.abspath(
    os.path.join(os.path.dirname( __file__ ), '..', 'config'))


# Load OpenAI API Key
api_key = os.environ.get('OPENAI_API_KEY')
if api_key is None:
    raise ValueError("OPENAI_API_KEY environment variable is not set.")

# Enable logging
log_format = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(filename='debug.log', level=logging.DEBUG, format=log_format)

# Suppress debug logs from OpenAI and requests libraries
logging.getLogger("openai").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# Loading config.yaml file
def load_config(config):
    path = os.path.join(CONFIG_BASE_PATH, config)
    with open(path, 'r') as stream:
        try:
            config_data = yaml.safe_load(stream)
            return Box(config_data)
        except yaml.YAMLError as exc:
            print(exc)
            return None






