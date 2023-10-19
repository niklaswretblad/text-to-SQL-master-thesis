
import os
import wandb
from utils.utils import load_json
import json
import shutil

api = wandb.Api()

PROJECT_NAME = "text-to-sql-generation"
ENTITY = "master-thesis-combientmix"

ARTIFACTS_PATH = os.path.abspath(
      os.path.join(os.path.dirname( __file__ ), '..', 'artifacts/'))

artifact_name = "query_results"

if not os.path.exists(ARTIFACTS_PATH):
    os.makedirs(ARTIFACTS_PATH)

# Download all artifacts and put them in the corresponding run folder
for run in api.runs(f"{ENTITY}/{PROJECT_NAME}"):        
    for i, artifact in enumerate(run.logged_artifacts()):        
        if artifact.name.startswith("query_results"):
            
            a_p = f"{ENTITY}/{PROJECT_NAME}/{artifact.name}"            
            artifact_ref = api.artifact(f"{ENTITY}/{PROJECT_NAME}/{artifact.name}")
            artifact_dir = artifact_ref.download()

            dest_path = os.path.join(ARTIFACTS_PATH, f"{run.name}")    
            os.rename(artifact_dir, dest_path)
            print(f"Downloaded {artifact_name} for run {run.name} to {dest_path}")
            
print("All artifacts downloaded!")
# Go through each table and reconstruct the data we are interested in 


for experiment in os.listdir(ARTIFACTS_PATH):
    table_path = os.path.join(ARTIFACTS_PATH, experiment, "query_results.table.json")
    
    reformatted_experiment = []
    table = load_json(table_path)
    
    for question in table['data']:                
        reformatted_question = {
            'question': question[0],
            'gold_sql': question[1],
            'predicted_sql': question[2],
            'success': question[3]
        }

        reformatted_experiment.append(reformatted_question)

    new_file_path = os.path.join(ARTIFACTS_PATH, experiment + ".json")
    with open(new_file_path, 'w', encoding='utf-8') as f:
        json.dump(reformatted_experiment, f, ensure_ascii=False, indent=4)
    

if os.path.exists(ARTIFACTS_PATH) and os.path.isdir(ARTIFACTS_PATH):    
    for item in os.listdir(ARTIFACTS_PATH):
        item_path = os.path.join(ARTIFACTS_PATH, item)
        
        if os.path.isdir(item_path):
            shutil.rmtree(item_path)

    
