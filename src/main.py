
import os
import sys
from data_interface import DataLoader
from utils import load_json
from langchain.chat_models import ChatOpenAI
from agents.zero_shot import ZeroShotAgent
import mlflow

QUESTIONS_PATH = os.path.abspath(
    os.path.join(os.path.dirname( __file__ ), '..', 'data/questions.json'))

ACCEPTED_DATABASES = [
    #'car_retails',
    'financial',
    # 'retail_world',
    #'retails'
]

def main():
    api_key = os.environ.get('OPENAI_API_KEY')
    if api_key is None:
        raise ValueError("OPENAI_API_KEY environment variable is not set.")
    
    llm = ChatOpenAI(
        openai_api_key=api_key, 
        model_name="gpt-3.5-turbo",
        temperature=0.0,
        request_timeout=20
    )

    questions = load_json(QUESTIONS_PATH)
    questions = [question for question in questions if question['db_id'] in ACCEPTED_DATABASES]
    questions = [question for question in questions if question['difficulty']=='simple']
    
    data_loader = DataLoader()    
    zero_shot_agent = ZeroShotAgent(llm)
    
    no_questions = len(questions)
    score = 0
    accuracy = 0
    with mlflow.start_run():
        for i, row in enumerate(questions):
            if row['db_id'] in ACCEPTED_DATABASES:
                golden_sql = row['SQL']
                db_id = row['db_id']            
                question = row['question']
                
                sql_schema = data_loader.get_create_statements(db_id)            
                predicted_sql = zero_shot_agent.generate_query(sql_schema, question)            

                success = data_loader.execute_query(predicted_sql, golden_sql, db_id)
                score += success
                
                accuracy = score / i
                mlflow.log_param("accuracy", accuracy)
                print("Percentage done: ", round(i / no_questions * 100, 2), "% Domain: ", db_id, " Success: ", success, " Accuracy: ", accuracy)

        # Log an artifact (output file)
        with open("output.txt", "w") as f:
            f.write("Hello, MLflow!")
        mlflow.log_artifact("output.txt")

    if __name__ == "__main__":
        main()