from langchain.llms import OpenAI
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from agents.base_agent import BaseAgent
from config import config

class ZeroShotAgent(BaseAgent):

    def __init__(self, llm):
        self.llm = llm
        prompt = PromptTemplate(    
            input_variables=["question", "database_schema"],
            template=config.templates.zero_shot_base_template,
        )
        self.chain = LLMChain(llm=llm, prompt=prompt)

    def generate_query(self, database_schema, question):
        return self.chain.run({
            'database_schema': database_schema,
            'question': question            
        })