
import os
import wandb
from utils.utils import load_json
import json
import shutil

api = wandb.Api()

PROJECT_NAME = "text-to-sql-generation"
ENTITY = "master-thesis-combientmix"

RESULTS_PATH = os.path.abspath(
      os.path.join(os.path.dirname( __file__ ), '..', 'results'))

ARTIFACTS_PATH = os.path.abspath(
      os.path.join(os.path.dirname( __file__ ), '..', 'results', 'artifacts'))


artifact_name = "query_results"

if not os.path.exists(RESULTS_PATH):
    os.makedirs(RESULTS_PATH)

if not os.path.exists(ARTIFACTS_PATH):
    os.makedirs(ARTIFACTS_PATH)

# Download all artifacts and put them in the corresponding run folder
for run in api.runs(f"{ENTITY}/{PROJECT_NAME}"):    
    for artifact in run.logged_artifacts():        
        if artifact.name.startswith("query_results"):                 
            artifact_ref = api.artifact(f"{ENTITY}/{PROJECT_NAME}/{artifact.name}")
            artifact_dir = artifact_ref.download()

            dest_path = os.path.join(RESULTS_PATH, f"{run.name}")
            os.rename(artifact_dir, dest_path)
            print(f"Downloaded {artifact_name} for run {run.name} to {dest_path}")
            
print("All artifacts downloaded!")


for experiment in os.listdir(RESULTS_PATH):
    if experiment != 'artifacts':
        table_path = os.path.join(RESULTS_PATH, experiment, "query_results.table.json")
        
        reformatted_experiment = []
        table = load_json(table_path)
        
        for question in table['data']:
            if (len(question) == 5):                
                reformatted_question = {
                    'question': question[0],
                    'gold_sql': question[1],
                    'predicted_sql': question[2],
                    'success': question[3],
                    'difficulty': question[4]
                }
            else:
                reformatted_question = {
                    'question': question[0],
                    'classified_quality': question[1],
                    'difficulty': question[2]
                }

            reformatted_experiment.append(reformatted_question)

        new_file_path = os.path.join(ARTIFACTS_PATH, experiment + ".json")
        with open(new_file_path, 'w', encoding='utf-8') as f:
            json.dump(reformatted_experiment, f, ensure_ascii=False, indent=4)
        

if os.path.exists(RESULTS_PATH) and os.path.isdir(RESULTS_PATH):
    for item in os.listdir(RESULTS_PATH):
        item_path = os.path.join(RESULTS_PATH, item)
        
        if os.path.isdir(item_path) and not 'artifacts' in item_path:
            shutil.rmtree(item_path)

rm_path = os.path.abspath(
      os.path.join(os.path.dirname( __file__ ), '..', 'artifacts'))

if os.path.exists(rm_path):
    shutil.rmtree(rm_path)
    
