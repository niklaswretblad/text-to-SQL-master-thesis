
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from langchain.callbacks import get_openai_callback
from agents.base_agent import BaseAgent
from config import config
from utils.timer import Timer
import logging
import wandb

class ZeroShotAgent(BaseAgent):
    total_tokens = 0
    prompt_tokens = 0 
    total_cost = 0
    completion_tokens = 0
    last_call_execution_time = 0
    total_call_execution_time = 0

    def __init__(self, llm):
        self.llm = llm
        prompt = PromptTemplate(    
            input_variables=["question", "database_schema","evidence"],
            template=config.current_prompt,
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
        
    