import os
from datasets import get_dataset
from langchain.chat_models import ChatOpenAI
from langchain.prompts import PromptTemplate

from config import api_key, load_config
import wandb
import langchain

CLASSIFIY_PROMPT = """
This instruction is regarding a text-to-SQL project where questions are being prompted to an LLM  
to predict the correct SQL-query for querying the correct data based on the question. 
However, some of the questions in the data are badly formulated.

A question is considered badly formulated if it is ambigiuous or in someway could result in a misinterpretation that leads to a 
incorrect predicted SQL-query.

A question is considered good formulated if it is clear, unambigious and would with high probability result in a correct SQL query.

Furthermore, you will be given the question’s corresponding gold SQL query (the true query). 
Given that information could you use that to decide if a question is good or badly formulated?

If you find a badly formulated questions please mark this question with the integer 0.
If you find a good formulated question please mark this question with the integer 1.

Do not return anything else than the mark as sole number and no corresponding text.

Question:
{question}
"""

class Classifier():
    
    total_tokens = 0
    prompt_tokens = 0 
    total_cost = 0
    completion_tokens = 0
    last_call_execution_time = 0
    total_call_execution_time = 0

    def __init__(self, llm):        
        self.llm = llm

        self.prompt_template = CLASSIFIY_PROMPT
        prompt = PromptTemplate(    
            input_variables=["question", "database_schema","evidence"],
            template=CLASSIFIY_PROMPT,
        )

        self.chain = LLMChain(llm=llm, prompt=prompt)


def generate_query(self, database_schema, question, evidence):
    with get_openai_callback() as cb:
        with Timer() as t:
            response = self.chain.run({
                'database_schema': database_schema,
                'question': question,
                "evidence": evidence
            })

        logging.info(f"OpenAI API execution time: {t.elapsed_time:.2f}")
        
        self.last_call_execution_time = t.elapsed_time
        self.total_call_execution_time += t.elapsed_time
        self.total_tokens += cb.total_tokens
        self.prompt_tokens += cb.prompt_tokens
        self.total_cost += cb.total_cost
        self.completion_tokens += cb.completion_tokens

        return response


def main():
    config = load_config("classifier_config.yaml")
    print('config: ', config.dataset)

    wandb.init(
        project=config.project,
        config=config,
        name=config.current_experiment,
        entity=config.entity
    )

    artifact = wandb.Artifact('query_results', type='dataset')
    table = wandb.Table(columns=["Question", "Classification", "Difficulty"]) ## Är det något mer vi vill ha med här?

     llm = ChatOpenAI(
        openai_api_key=api_key, 
        model_name=config.llm_settings.model,
        temperature=config.llm_settings.temperature,
        request_timeout=config.llm_settings.request_timeout
    )

    dataset = get_dataset(config.dataset)
    classifier = Classifier(llm)

    wandb.config['prompt'] = classifier.prompt_template

    no_data_points = dataset.get_number_of_data_points()
    no_correct = 0
    no_incorrect = 0
    
    for i in range(no_data_points):
        data_point = dataset.get_data_point(i)
        evidence = data_point['evidence']
        # golden_sql = data_point['SQL']
        db_id = data_point['db_id']            
        question = data_point['question']
        difficulty = ""
        
        sql_schema = dataset.get_schema_and_sample_data(db_id)
        
        if (config.dataset == "BIRD" or 
            config.dataset == "BIRDFixedFinancial" or 
            config.dataset == "BIRDExperimentalFinancial" or 
            config.dataset == "BIRDFixedFinancialGoldSQL"):

            # bird_table_info = dataset.get_bird_db_info(db_id)
            # sql_schema = sql_schema + bird_table_info

            if 'difficulty' in data_point:
                difficulty = data_point['difficulty']
        else:
            bird_table_info = ""

        classification = classifier.generate_query(database_schema, question, evidence)   

        if (classification == 1):
            no_correct += 1
        else:
            no_incorrect += 1

        table.add_data(question, classification, difficulty)
        wandb.log({
            "no_correct": no_correct,
            "no_incorrect": no_incorrect
            "total_tokens": zero_shot_agent.total_tokens,
            "prompt_tokens": zero_shot_agent.prompt_tokens,
            "completion_tokens": zero_shot_agent.completion_tokens,
            "total_cost": zero_shot_agent.total_cost,
            "openAPI_call_execution_time": zero_shot_agent.last_call_execution_time,
        }, step=i+1)
    
        print("Correct/incorrect (1/0): ", classification)
        
    
    wandb.run.summary['number_of_questions']                = dataset.get_number_of_data_points()
    wandb.run.summary["total_tokens"]                       = zero_shot_agent.total_tokens
    wandb.run.summary["prompt_tokens"]                      = zero_shot_agent.prompt_tokens
    wandb.run.summary["completion_tokens"]                  = zero_shot_agent.completion_tokens
    wandb.run.summary["total_cost"]                         = zero_shot_agent.total_cost
    wandb.run.summary['total_predicted_execution_time']     = dataset.total_predicted_execution_time
    wandb.run.summary['total_openAPI_execution_time']       = zero_shot_agent.total_call_execution_time

    artifact.add(table, "query_results")
    wandb.log_artifact(artifact)

    artifact_code = wandb.Artifact('code', type='code')
    artifact_code.add_file("src/run_classifier.py")
    wandb.log_artifact(artifact_code)

    wandb.finish()



if __name__ == "__main__":
    main()