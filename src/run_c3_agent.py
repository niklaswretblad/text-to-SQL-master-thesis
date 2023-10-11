
import os
from datasets import get_dataset
from langchain.chat_models import ChatOpenAI
from agents.few_shot import FewShotAgent
from config import api_key, load_config
import wandb
import langchain 
langchain.verbose = False

## C3 imports
import argparse
import json
import time
import openai
from agents.c3_zero_shot.sql_post_process import fix_select_column
import re
import os
import sqlite3
from agents.c3_zero_shot.get_selfconsistent_output import get_sqls
from tqdm import tqdm
from agents.c3_zero_shot.c3_agent import parse_option, C3_PROMPT, generate_reply,is_valid 

# If you don't want your script to sync to the cloud
os.environ["WANDB_MODE"] = "offline"

def main():
    config = load_config("few_shot_config.yaml")

    opt = parse_option()
    print(opt)
    with open(opt.input_dataset_path) as f:
        data = json.load(f)
    results = []
    p_sql_final = []
    if not opt.self_consistent:
        for i, item in enumerate(data):
            print("id", i)
            db_dir = opt.db_dir + '/' + item['db_id'] + '/' + item['db_id'] + '.sqlite'
            for j in range(5):
                messages = []
                messages = C3_PROMPT.copy()
                input = item['input_sequence']
                messages.append({"role": "user", "content": input})
                p_sql = generate_reply(messages, 1)[0]
                p_sql = 'SELECT ' + p_sql
                p_sql = p_sql.replace("SELECT SELECT", "SELECT")
                p_sql = fix_select_column(p_sql)
                p_sql = p_sql.replace("> =", ">=").replace("< =", "<=").replace("! =", "!=")
                print(f'p_sql: {p_sql}')
                if is_valid(p_sql, db_dir):
                    break
                else:
                    print(f're_id: {j} p_sql: {p_sql} exec error...')
                    time.sleep(0.5)
                    if j < 4:
                        print(f'generate again')
            p_sql_final.append(p_sql)
            print(p_sql_final)
    else:
        for i, item in enumerate(tqdm(data)):
            db_dir = opt.db_dir + '/' + item['db_id'] + '/' + item['db_id'] + '.sqlite'
            p_sqls = []
            for j in range(5):
                messages = []
                messages = C3_PROMPT.copy()
                input = item['input_sequence']
                messages.append({"role": "user", "content": input})
                reply = None
                while reply is None:
                    try:
                        reply = generate_reply(messages, opt.n)
                    except Exception as e:
                        print(e)
                        print(f"api error, wait for 3 seconds and retry...")
                        time.sleep(3)
                        pass
                p_sqls = reply
                temp = []
                for p_sql in p_sqls:
                    p_sql = 'SELECT ' + p_sql
                    p_sql = p_sql.replace("SELECT SELECT", "SELECT")
                    try:
                        p_sql = fix_select_column(p_sql)
                    except:
                        print(f"fix_select_column err, p_sql: {p_sql}")
                        pass
                    p_sql = p_sql.replace("> =", ">=").replace("< =", "<=").replace("! =", "!=")
                    p_sql = p_sql.replace("\n", " ")
                    while "  " in p_sql:
                        p_sql = p_sql.replace("  ", " ")
                    temp.append(p_sql)
                p_sqls = temp
                if is_valid(p_sqls[0], db_dir):
                    break
                else:
                    print(f're_id: {j} p_sql: {p_sqls[0]} exec error...')
                    time.sleep(0.5)
                    if j < 4:
                        print(f'generate again')
            result = {}
            result['db_id'] = item['db_id']
            result['question'] = item['question']
            result['p_sqls'] = []
            for sql in p_sqls:
                result['p_sqls'].append(sql)
            results.append(result)
            # time.sleep(1)
        p_sql_final = get_sqls(results, opt.n, opt.db_dir)
    with open(opt.output_dataset_path, 'w') as f:
        for sql in p_sql_final:
            print(sql, file=f)



### Generic Agent Code ####
    # wandb.init(
    #     project=config.project,
    #     config=config,
    #     name= config.current_experiment,
    #     entity=config.entity
    # )

    # artifact = wandb.Artifact('query_results', type='dataset')
    # table = wandb.Table(columns=["Question", "Gold Query", "Predicted Query", "Success"])    

    # wandb.define_metric("predicted_sql_execution_time", summary="mean")
    # wandb.define_metric("gold_sql_execution_time", summary="mean")

    # llm = ChatOpenAI(
    #     openai_api_key=api_key, 
    #     model_name=config.llm_settings.model,
    #     temperature=config.llm_settings.temperature,
    #     request_timeout=config.llm_settings.request_timeout
    # )

    # dataset = get_dataset(config.dataset)    
    # few_shot_agent = FewShotAgent(llm)

    # wandb.config['prompt'] = few_shot_agent.prompt_template
    
    # no_data_points = dataset.get_number_of_data_points()
    # score = 0
    # accuracy = 0
    # for i in range(no_data_points):
    #     # if i == 5 or i == 26 or i == 27:
    #     #     continue

    #     data_point = dataset.get_data_point(i)
    #     evidence = data_point['evidence']
    #     golden_sql = data_point['SQL']
    #     db_id = data_point['db_id']            
    #     question = data_point['question']

    #     sql_schema = dataset.get_schema_and_sample_data(db_id)
    #     predicted_sql = few_shot_agent.generate_query(sql_schema, question, evidence)        
    #     success = dataset.execute_queries_and_match_data(predicted_sql, golden_sql, db_id)

    #     score += success
    #     accuracy = score / (i + 1)

    #     table.add_data(question, golden_sql, predicted_sql, success)
    #     wandb.log({
    #         "accuracy": accuracy,
    #         "total_tokens": few_shot_agent.total_tokens,
    #         "prompt_tokens": few_shot_agent.prompt_tokens,
    #         "completion_tokens": few_shot_agent.completion_tokens,
    #         "total_cost": few_shot_agent.total_cost,
    #         "openAPI_call_execution_time": few_shot_agent.last_call_execution_time,
    #         "predicted_sql_execution_time": dataset.last_predicted_execution_time,
    #         "gold_sql_execution_time": dataset.last_gold_execution_time
    #     }, step=i+1)
    
    #     print("Percentage done: ", round(i / no_data_points * 100, 2), "% Domain: ", 
    #           db_id, " Success: ", success, " Accuracy: ", accuracy)
            

    # wandb.run.summary['number_of_questions']                = no_data_points
    # wandb.run.summary["accuracy"]                           = accuracy
    # wandb.run.summary["total_tokens"]                       = few_shot_agent.total_tokens
    # wandb.run.summary["prompt_tokens"]                      = few_shot_agent.prompt_tokens
    # wandb.run.summary["completion_tokens"]                  = few_shot_agent.completion_tokens
    # wandb.run.summary["total_cost"]                         = few_shot_agent.total_cost
    # wandb.run.summary['total_predicted_execution_time']     = dataset.total_predicted_execution_time
    # wandb.run.summary['total_gold_execution_time']          = dataset.total_gold_execution_time
    # wandb.run.summary['total_openAPI_execution_time']       = few_shot_agent.total_call_execution_time

    # artifact.add(table, "query_results")
    # wandb.log_artifact(artifact)

    # artifact_code = wandb.Artifact('code', type='code')
    # artifact_code.add_file("src/agents/c3_agent.py")
    # wandb.log_artifact(artifact_code)

    # wandb.finish()



if __name__ == "__main__":
    main()