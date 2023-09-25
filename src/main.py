
import os
from db_interface import *
from utils import load_json
from langchain.chat_models import ChatOpenAI
import os
from agents.zero_shot import ZeroShotAgent

QUESTIONS_PATH = os.path.abspath(
    os.path.join(os.path.dirname( __file__ ), '..', 'data/questions.json'))

ACCEPTED_DATABASES = [
    'car_retails',
    'financial',
    # 'retail_world',
    #'retails'
]

def main():
    api_key = os.environ.get('OPENAI_API_KEY')
    if api_key is None:
        raise ValueError("OPENAI_API_KEY environment variable is not set.")

    llm = ChatOpenAI(openai_api_key=api_key, model_name="gpt-3.5-turbo",temperature=0.9)

    questions = load_json(QUESTIONS_PATH)
    questions = [question for question in questions if question['db_id'] in ACCEPTED_DATABASES]
    db_loader = DBLoader(ACCEPTED_DATABASES)
    zero_shot_agent = ZeroShotAgent(llm)

    score = 0
    total_questions = len(questions)
    for i, row in enumerate(questions):
        if row['db_id'] in ACCEPTED_DATABASES:
            golden_sql = row['SQL']
            db_id = row['db_id']
            question = row['question']

            predicted_sql = zero_shot_agent.generate_query(question)
            print('Predicted query: ', predicted_sql)
            break

            res = db_loader.execute_query(golden_sql, golden_sql, db_id)
            score += res

            print("Percentage done: ", round(i / total_questions * 100, 2), "% Domain: ", db_id)

    print("accuracy: ", score / len(questions))

if __name__ == "__main__":
    main()