
import os
from data_interface import DataLoader
from utils.utils import load_json
from langchain.chat_models import ChatOpenAI
from agents.zero_shot import ZeroShotAgent
from config import config, api_key
import wandb

QUESTIONS_PATH = os.path.abspath(
    os.path.join(os.path.dirname( __file__ ), '../data/questions.json'))

# If you don't want your script to sync to the cloud
os.environ["WANDB_MODE"] = "offline"

def main():
    
    wandb.init(
        project=config.project,
        config=config,
        name= config.current_experiment,
        entity=config.entity
    )

    artifact = wandb.Artifact('query_results', type='dataset')
    table = wandb.Table(columns=["Question", "Gold Query", "Predicted Query", "Success"])

    wandb.define_metric("total_tokens", summary="last")
    wandb.define_metric("prompt_tokens", summary="last")
    wandb.define_metric("completion_tokens", summary="last")
    wandb.define_metric("total_tokens", summary="last")
    wandb.define_metric("llm_api_execution_time", summary="last")

    wandb.define_metric("predicted_sql_execution_time", summary="mean")
    wandb.define_metric("gold_sql_execution_time", summary="mean")

    wandb.define_metric("cumulative_gold_sql_execution_time", summary="last")
    wandb.define_metric("cumulative_predicted_sql_execution_time", summary="last")

    llm = ChatOpenAI(
        openai_api_key=api_key, 
        model_name=config.llm_settings.model,
        temperature=config.llm_settings.temperature,
        request_timeout=config.llm_settings.request_timeout
    )

    questions = load_json(QUESTIONS_PATH)
    questions = [question for question in questions if question['db_id'] in config.domains]
    questions = [question for question in questions if question['difficulty'] in config.difficulties]

    wandb.run.summary['number_of_questions'] = len(questions)
    
    data_loader     = DataLoader()    
    zero_shot_agent = ZeroShotAgent(llm)
    
    no_questions = len(questions)
    score = 0
    accuracy = 0
    for i, row in enumerate(questions):        
        golden_sql = row['SQL']
        db_id = row['db_id']            
        question = row['question']
        evidence = row['evidence']
        difficulty = row['difficulty']

        sql_schema = data_loader.get_create_statements(db_id)
        predicted_sql = zero_shot_agent.generate_query(sql_schema, question, evidence)        
        success = data_loader.execute_queries_and_match_data(predicted_sql, golden_sql, db_id)

        score += success
        if i > 0: accuracy = score / i                

        table.add_data(question, golden_sql, predicted_sql, success)
        wandb.log({"accuracy": accuracy,
            "total_tokens": zero_shot_agent.total_tokens,
            "prompt_tokens": zero_shot_agent.prompt_tokens,
            "completion_tokens": zero_shot_agent.completion_tokens,
            "total_cost": zero_shot_agent.total_cost,
            "difficulty": difficulty
        })
    
        print("Percentage done: ", round(i / no_questions * 100, 2), "% Domain: ", 
              db_id, " Success: ", success, " Accuracy: ", accuracy)
        
        # if i == 10:
        #     break
    
    artifact.add(table, "query_results")
    wandb.log_artifact(artifact)

    artifact_code = wandb.Artifact('code', type='code')
    artifact_code.add_file("src/agents/zero_shot.py")
    wandb.log_artifact(artifact_code)

    wandb.finish()



if __name__ == "__main__":
    main()