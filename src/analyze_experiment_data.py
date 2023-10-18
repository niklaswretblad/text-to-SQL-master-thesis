
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
    
    results = []
    for entry in data:
        gold_sql = entry["gold_sql"]
        predicted_sql = entry['predicted_sql']
        result = {
            "question": entry["question"],
            "golden_joins": count_joins(gold_sql),
            "golden_subqueries": count_subqueries(gold_sql),
            "golden_tables": get_tables(gold_sql),
            "golden_group_by": count_group_by(gold_sql),
            "predicted_joins": count_joins(predicted_sql),
            "predicted_subqueries": count_subqueries(predicted_sql),
            "predicted_tables": get_tables(predicted_sql),
            "predicted_group_by": count_group_by(predicted_sql)
        }
        # TODO
        # Nested joins
        # Count
        # Predicted query
        # Golden query
        # success


        results.append(result)
    return results

def save_to_csv(data, output_file):
    with open(output_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["question", "golden_joins", "golden_subqueries", "golden_tables", "golden_group_by",
                                               "predicted_joins", "predicted_subqueries", "predicted_tables", "predicted_group_by"])
        writer.writeheader()
        for row in data:
            writer.writerow(row)


if not os.path.exists(ARTIFACTS_PATH):
    os.makedirs(ARTIFACTS_PATH)

for experiment in os.listdir(ARTIFACTS_PATH):
    file_path = os.path.join(ARTIFACTS_PATH, experiment)
    experiment_name = experiment.replace(".json", "")
    output_csv = os.path.join(RESULTS_PATH, f"results_{experiment_name}.csv")
    results = process_experiment_file(file_path)
    save_to_csv(results, output_csv)

        


