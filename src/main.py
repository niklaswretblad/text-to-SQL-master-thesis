
import os
import sys
from data_interface import DataLoader
from utils import load_json
from langchain.chat_models import ChatOpenAI
from agents.zero_shot import ZeroShotAgent
import mlflow
from mlflow.tracking import MlflowClient
from config import config

QUESTIONS_PATH = os.path.abspath(
    os.path.join(os.path.dirname( __file__ ), '../data/questions.json'))

CONFIG_PATH = os.path.abspath(
    os.path.join(os.path.dirname( __file__ ), '../config/config.yaml'))

def main():
    mlflow.set_experiment(config.current_experiment)
    
    api_key = os.environ.get('OPENAI_API_KEY')
    if api_key is None:
        raise ValueError("OPENAI_API_KEY environment variable is not set.")
    
    llm = ChatOpenAI(
        openai_api_key=api_key, 
        model_name=config.llm_settings.model,
        temperature=config.llm_settings.temperature,
        request_timeout=config.llm_settings.request_timeout
    )

    questions = load_json(QUESTIONS_PATH)
    questions = [question for question in questions if question['db_id'] in config.domains]
    questions = [question for question in questions if question['difficulty'] in config.difficulties]
    
    data_loader = DataLoader()    
    zero_shot_agent = ZeroShotAgent(llm)
    
    no_questions = len(questions)
    score = 0
    accuracy = 0
    mlflow.end_run()

    with mlflow.start_run() as run:        
        for i, row in enumerate(questions):        
            mlflow.log_artifact(CONFIG_PATH)

            golden_sql = row['SQL']
            db_id = row['db_id']            
            question = row['question']
            
            sql_schema = data_loader.get_create_statements(db_id)            
            result = zero_shot_agent.generate_query(sql_schema, question, i)            

            success = data_loader.execute_query(result['sql'], golden_sql, db_id)
            score += success
            
            if i > 0: accuracy = score / i                
            print("Percentage done: ", round(i / no_questions * 100, 2), "% Domain: ", db_id, " Success: ", success, " Accuracy: ", accuracy)

            if i == 10:
                break

        mlflow.log_metric("accuracy", accuracy)

        mlflow.end_run()

if __name__ == "__main__":
    main()