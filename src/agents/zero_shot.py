from langchain.llms import OpenAI
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from langchain.callbacks import get_openai_callback
from agents.base_agent import BaseAgent
from config import config
from timer import Timer
import mlflow


class ZeroShotAgent(BaseAgent):

    def __init__(self, llm):
        self.llm = llm
        prompt = PromptTemplate(    
            input_variables=["question", "database_schema"],
            template=config.prompt_templates.zero_shot_base_template,
        )
        self.chain = LLMChain(llm=llm, prompt=prompt)

    def generate_query(self, database_schema, question, step):
        with get_openai_callback() as cb:
            with Timer("generate_query()"):
                response = self.chain.run({
                    'database_schema': database_schema,
                    'question': question            
                })            
            
            return {
                'sql': response,
                'total_tokens': cb.total_tokens,
                'prompt_tokens': cb.prompt_tokens,
                'total_cost': cb.total_cost
            }