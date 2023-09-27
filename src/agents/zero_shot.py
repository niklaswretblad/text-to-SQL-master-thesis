from langchain.llms import OpenAI
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from langchain.callbacks import get_openai_callback
from agents.base_agent import BaseAgent
from config import config
from timer import Timer
import mlflow


class ZeroShotAgent(BaseAgent):
    total_tokens = 0
    prompt_tokens = 0 
    total_cost = 0
    completion_tokens = 0

    def __init__(self, llm):
        self.llm = llm
        prompt = PromptTemplate(    
            input_variables=["question", "database_schema"],
            template=config.prompt_templates.zero_shot_base_template,
        )
        self.chain = LLMChain(llm=llm, prompt=prompt)

    def generate_query(self, database_schema, question):
        with get_openai_callback() as cb:
            with Timer("generate_query()"):
                response = self.chain.run({
                    'database_schema': database_schema,
                    'question': question            
                })            
            
            self.total_tokens += cb.total_tokens
            self.prompt_tokens += cb.prompt_tokens
            self.total_cost += cb.total_cost
            self.completion_tokens += cb.completion_tokens

            return response
        
    