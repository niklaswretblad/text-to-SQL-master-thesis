from langchain.llms import OpenAI
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
import os
from agents.base_agent import BaseAgent

ZERO_SHOT_PROMPT = """
Database schema in the form of CREATE_TABLE statements:
{database_schema}
-- Using valid SQLite, answer the following questions for the tables provided above. 
Question: {question}
DO NOT return anything else except the SQL statement. 
"""

class ZeroShotAgent(BaseAgent):

    def __init__(self, llm):
        self.llm = llm
        prompt = PromptTemplate(
            input_variables=["question", "database_schema"],
            template=ZERO_SHOT_PROMPT,
        )
        self.chain = LLMChain(llm=llm, prompt=prompt)

    def generate_query(self, database_schema, question):
        return self.chain.run({
            'database_schema': database_schema,
            'question': question            
        })