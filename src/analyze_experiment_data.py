
import re
import os
import json
import csv


ARTIFACTS_PATH = os.path.abspath(
      os.path.join(os.path.dirname( __file__ ), '..', 'results', 'artifacts'))


RESULTS_PATH = os.path.abspath(
      os.path.join(os.path.dirname( __file__ ), '..', 'results'))


def count_joins(sql):
    return len(re.findall(r'JOIN', sql, re.I))

def count_subqueries(sql):
    return len(re.findall(r'\(SELECT', sql, re.I))

def count_counts(sql):
    return len(re.findall(r'COUNT\(', sql, re.I))

def get_tables(sql):
    # Extract tables in FROM or JOIN clauses
    tables = re.findall(r'FROM\s+([\w]+)|JOIN\s+([\w]+)', sql, re.I)
    # Flatten the list of tuples and remove None values
    return list(filter(None, [table for sub in tables for table in sub]))

def count_group_by(sql):
    return len(re.findall(r'GROUP\s+BY', sql, re.I))

def process_experiment_file(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    columns = []
    results = []    
    for entry in data:
        print('length of entry: ',len(entry))
        if (len(entry) == 5):
            gold_sql = entry["gold_sql"]
            predicted_sql = entry['predicted_sql']
            predicted_sql = predicted_sql.replace("\n", " ")
            difficulty = entry['difficulty'] if 'difficulty' in entry else ""
            result = {
                "question": entry["question"],
                "gold_sql": gold_sql,
                "predicted_sql": predicted_sql,
                "success": entry['success'],
                'difficulty': difficulty,
                "gold_tables": get_tables(gold_sql),
                "predicted_tables": get_tables(predicted_sql)                        
            }
        else:
            difficulty = entry['difficulty'] if 'difficulty' in entry else ""
            result = {
                "question": entry["question"],
                "classified_quality": entry["classified_quality"],
                'difficulty': difficulty,
            }
        results.append(result)

    if len(results) > 0:
        columns = list(results[0].keys())

        
    return (results, columns)

def save_to_csv(data, output_file, columns):
    with open(output_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in data:
            writer.writerow(row)


if not os.path.exists(ARTIFACTS_PATH):
    os.makedirs(ARTIFACTS_PATH)

for experiment in os.listdir(ARTIFACTS_PATH):
    file_path = os.path.join(ARTIFACTS_PATH, experiment)
    experiment_name = experiment.replace(".json", "")
    output_csv = os.path.join(RESULTS_PATH, f"results_{experiment_name}.csv")
    results, columns = process_experiment_file(file_path)
    save_to_csv(results, output_csv, columns)

        


