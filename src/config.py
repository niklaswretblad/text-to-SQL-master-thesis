
import os
import yaml
from box import Box
import logging

CONFIG_PATH = os.path.abspath(
    os.path.join(os.path.dirname( __file__ ), '../config/config.yaml'))

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


# Change MLFlow paths to enable sharing the same ./mlruns folder
def replace_path_in_yaml(yaml_file, new_path_prefix):
    with open(yaml_file, 'r') as file:
        data = yaml.safe_load(file)

    modified = False
    for key in ["artifact_uri", "artifact_location"]:
        if key in data:
            # Split the path at the first occurrence of /mlruns/ and replace the prefix
            parts = data[key].split('/mlruns/', 1)
            if len(parts) > 1:
                data[key] = "file://" + os.path.join(new_path_prefix, 'mlruns', parts[1])
                modified = True

    if modified:
        with open(yaml_file, 'w') as file:
            yaml.dump(data, file)

def update_meta_yaml_paths(root_directory):
    """Update meta.yaml files in the mlruns directory within the given root_directory."""
    mlruns_dir = os.path.join(root_directory, 'mlruns')

    # Walk through the directory to find meta.yaml files
    for dirpath, dirnames, filenames in os.walk(mlruns_dir):
        for filename in filenames:
            if filename == "meta.yaml":
                replace_path_in_yaml(os.path.join(dirpath, filename), root_directory)

cwd = os.getcwd()
update_meta_yaml_paths(cwd)




