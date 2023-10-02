from langchain.llms import OpenAI
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from langchain.callbacks import get_openai_callback
from agents.base_agent import BaseAgent
from config import config
from timer import Timer
import wandb

PROMPT_TEMPLATE = """Database schema in the form of CREATE_TABLE statements:
{database_schema}
-- Using valid SQLite, answer the following question based on the tables provided above. 
Hint helps you to write the correct sqlite SQL query.
Question: {question}
Hint: {evidence}
DO NOT return anything else except the SQL statement."""

class ZeroShotAgent(BaseAgent):
    total_tokens = 0
    prompt_tokens = 0 
    total_cost = 0
    completion_tokens = 0

    def __init__(self, llm):
        self.llm = llm
        prompt = PromptTemplate(    
            input_variables=["question", "database_schema","evidence"],
            template=PROMPT_TEMPLATE,
        )
        self.chain = LLMChain(llm=llm, prompt=prompt)

    def generate_query(self, database_schema, question, evidence):
        with get_openai_callback() as cb:
            with Timer("generate_query()"):
                response = self.chain.run({
                    'database_schema': database_schema,
                    'question': question,
                    "evidence": evidence
                                
                })       

            print('evidence: ', evidence)
            self.total_tokens += cb.total_tokens
            self.prompt_tokens += cb.prompt_tokens
            self.total_cost += cb.total_cost
            self.completion_tokens += cb.completion_tokens

            return response
        
    