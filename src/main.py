
from db_interface import *

QUESTIONS_PATH = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'data/questions.json'))


ACCEPTED_DATABASES = [
    'car_retails',
    'financial',
    # 'retail_world',
    'retails'
]

def main():
    questions = load_json(QUESTIONS_PATH)
    questions = [question for question in questions if question['db_id'] in ACCEPTED_DATABASES]
    db_loader = DBLoader(ACCEPTED_DATABASES)

    score = 0
    total_questions = len(questions)
    for i, row in enumerate(questions):
        if row['db_id'] in ACCEPTED_DATABASES:
            golden_sql = row['SQL']
            db_id = row['db_id']
            
            res = db_loader.execute_query(golden_sql, golden_sql, db_id)
            score += res

            print("Percentage done: ", round(i / total_questions * 100, 2), "% Domain: ", db_id)

    #list_tables_and_columns(DB_BASE_PATH + '/' + 'retail_world' + '/' + 'retail_world' + '.sqlite')

    #print("accuracy: ", score / len(questions))
if __name__ == "__main__":
    main()