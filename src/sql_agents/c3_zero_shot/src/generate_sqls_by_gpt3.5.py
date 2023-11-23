import argparse
import json
import time
import openai
from sql_post_process import fix_select_column
import re
import os
import sqlite3
from get_selfconsistent_output import get_sqls
from tqdm import tqdm

## Modified imports
import sys
sys.path.append('/Users/fredrik/code/project/Text-to-SQL-Generation/src')
from config import load_config
from datasets import SpiderDataset
import os
# from datasets import get_dataset
from langchain.chat_models import ChatOpenAI
from agents.din_sql import DinSQLAgent
from config import api_key, load_config
import wandb
import langchain
langchain.verbose = False


# add your openai api key
openai.api_key = os.environ.get('OPENAI_API_KEY')
log_cost = 0

spiderDataset=SpiderDataset()

chat_prompt = [
    {
        "role": "system",
        "content": "You are now an excellent SQL writer, first I'll give you some tips and examples, and I need you to remember the tips, and do not make same mistakes."
    },
    {
        "role": "user",
        "content": """Tips 1: 
Question: Which A has most number of B?
Gold SQL: select A from B group by A order by count ( * ) desc limit 1;
Notice that the Gold SQL doesn't select COUNT(*) because the question only wants to know the A and the number should be only used in ORDER BY clause, there are many questions asks in this way, and I need you to remember this in the the following questions."""
    },
    {
        "role": "assistant",
        "content": "Thank you for the tip! I'll keep in mind that when the question only asks for a certain field, I should not include the COUNT(*) in the SELECT statement, but instead use it in the ORDER BY clause to sort the results based on the count of that field."
    },
    {
        "role": "user",
        "content": """Tips 2: 
Don't use "IN", "OR", "LEFT JOIN" as it might cause extra results, use "INTERSECT" or "EXCEPT" instead, and remember to use "DISTINCT" or "LIMIT" when necessary.
For example, 
Question: Who are the A who have been nominated for both B award and C award?
Gold SQL should be: select A from X where award = 'B' intersect select A from X where award = 'C';"""
    },
    {
        "role": "assistant",
        "content": "Thank you for the tip! I'll remember to use \"INTERSECT\" or \"EXCEPT\" instead of \"IN\", \"OR\", or \"LEFT JOIN\" when I want to find records that match or don't match across two tables. Additionally, I'll make sure to use \"DISTINCT\" or \"LIMIT\" when necessary to avoid repetitive results or limit the number of results returned."
    }
]

column_recall_prompt = """
Given the database tables and question, perform the following actions: 
1 - Rank the columns in each table based on the possibility of being used in the SQL, Column that matches more with the question words or the foreign key is highly relevant and must be placed ahead. You should output them in the order of the most relevant to the least relevant.
Explain why you choose each column.
2 - Output a JSON object that contains all the columns in each table according to your explanation. The format should be like: 
{
    "table_1": ["column_1", "column_2", ......], 
    "table_2": ["column_1", "column_2", ......],
    "table_3": ["column_1", "column_2", ......],
     ......
}"""

table_recall_prompt ="""
Given the database schema and question, perform the following actions: 
1 - Rank all the tables based on the possibility of being used in the SQL according to the question from the most relevant to the least relevant, Table or its column that matches more with the question words is highly relevant and must be placed ahead.
2 - Check whether you consider all the tables.
3 - Output a list object in the order of step 2, Your output should contain all the tables. The format should be like: 
[
    "table_1", "table_2", ...
]
"""

def parse_option():
    parser = argparse.ArgumentParser("command line arguments for generate sqls")
    parser.add_argument("--input_dataset_path", type=str)
    parser.add_argument("--self_consistent", type=bool, default=True)
    parser.add_argument("--n", type=int, default=20,
                        help="Size of self-consistent set")
    parser.add_argument("--output_dataset_path", type=str)
    parser.add_argument("--db_dir", type=str, default="./data/database")

    opt = parser.parse_args()

    return opt


def generate_reply(messages, n,index, type):
    completions = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=messages,
        n=n,
    )
    token_input_cost = 0.0015/1000
    token_output_cost = 0.002/1000
    print('current index in generate_reply: ', index)
    global log_cost

    if type == "sc":
        if index==0:
            log_cost = 0
        print('self consistent')
        input_cost = completions["usage"]["prompt_tokens"]*token_input_cost
        output_cost = completions["usage"]["completion_tokens"]*token_output_cost
        total_cost = input_cost+output_cost
        log_cost += total_cost
        # wandb.log({"C3 Self-Consistency Prompt Cost": log_cost, "SC_prompt_step": index+1})
        print('Logging the price after each completion')
        print('Prompt cost: ', total_cost)
        print('Culiminative cost: ', log_cost, ' $ ')
    else:
        if index==0:
            log_cost = 0
        print('text to sql')
        input_cost = completions["usage"]["prompt_tokens"]*token_input_cost
        output_cost = completions["usage"]["completion_tokens"]*token_output_cost
        total_cost = input_cost+output_cost
        log_cost += total_cost
        # wandb.log({"Text-to-SQL Prompt Cost": log_cost, "Text_to_sql_step": index+1})
        print('Logging the price after each completion')
        print('Prompt cost: ', total_cost)
        print('Culiminative cost: ', log_cost, ' $ ')

    

    mes = completions.choices[0].message.content
    all_p_sqls = []
    for i in range(n):
        all_p_sqls.append(completions.choices[i].message.content.replace("\n", " "))
    return all_p_sqls


def replace_cur_year(query: str) -> str:
    return re.sub(
        "YEAR\s*\(\s*CURDATE\s*\(\s*\)\s*\)\s*", "2020", query, flags=re.IGNORECASE
    )


def get_cursor_from_path(sqlite_path: str):
    try:
        if not os.path.exists(sqlite_path):
            print("Openning a new connection %s" % sqlite_path)
        connection = sqlite3.connect(sqlite_path)
    except Exception as e:
        print(sqlite_path)
        raise e
    connection.text_factory = lambda b: b.decode(errors="ignore")
    cursor = connection.cursor()
    return cursor


def exec_on_db_(sqlite_path: str, query: str):
    query = replace_cur_year(query)
    cursor = get_cursor_from_path(sqlite_path)
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        cursor.connection.close()
        return "result", result
    except Exception as e:
        cursor.close()
        cursor.connection.close()
        return "exception", e


def is_valid(sql, db_path):
    flag, _ = exec_on_db_(db_path, sql)
    if flag == "exception":
        return 0
    else:
        return 1

def main():
    config = load_config("/Users/fredrik/code/project/Text-to-SQL-Generation/config/c3_config.yaml")

    wandb.init(
    project=config.project,
    config=config,
    name= config.current_experiment,
    entity=config.entity,
    id=config.run_id,
    resume="allow"
    )

    wandb.define_metric("C3 Self-Consistency Prompt Cost", step_metric="SC_prompt_step")
    wandb.define_metric("Text-to-SQL Prompt Cost", step_metric="Text_to_sql_step")
    wandb.define_metric("accuracy", step_metric="accuracy_step")


    artifact = wandb.Artifact('query_results', type='dataset')
    table = wandb.Table(columns=["Question", "Gold Query", "Predicted Query", "Success"]) 

    opt = parse_option()
    print('opt: ', opt)

    with open(opt.input_dataset_path) as f:
        data = json.load(f)
    results = []
    p_sql_final = []
    gold_sql = []
    if not opt.self_consistent:
        for i, item in enumerate(data):
            print("id", i)
            db_dir = opt.db_dir + '/' + item['db_id'] + '/' + item['db_id'] + '.sqlite'
            for j in range(5):
                messages = []
                messages = chat_prompt.copy()
                input = item['input_sequence']
                messages.append({"role": "user", "content": input})
                p_sql = generate_reply(messages, 1, i+j, type="sc")[0]
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
                messages = chat_prompt.copy()
                input = item['input_sequence']
                messages.append({"role": "user", "content": input})
                reply = None
                while reply is None:
                    try:
                        reply = generate_reply(messages, opt.n, i, type="normal")
                    except Exception as e:
                        print("main_file")
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
            result['gold_sql'] = item['query']
            result['question'] = item['question']
            result['p_sqls'] = []


            for sql in p_sqls:
                result['p_sqls'].append(sql)
            results.append(result)
            # time.sleep(1)
        p_sql_final = get_sqls(results, opt.n, opt.db_dir)
    with open(opt.output_dataset_path, 'w') as f:

        score = 0
        accuracy = 0
    
        for index, result in enumerate(results):
            
            success = spiderDataset.execute_queries_and_match_data(p_sql_final[index], result['gold_sql'], "small_bank_1")
            table.add_data(result['question'], result['gold_sql'], p_sql_final[index], success)
            print('sucess result: ', success)
            score += success
            accuracy = score / (index + 1)
            print('current accuracy: ', accuracy)
            wandb.log({"accuracy": accuracy}, step=index+1)
        wandb.run.summary["accuracy"] = accuracy
        artifact.add(table, "query_results")
        wandb.log_artifact(artifact)

        for sql in p_sql_final:
            print(sql, file=f)

if __name__ == '__main__':
    main()