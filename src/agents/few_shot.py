from langchain.llms import OpenAI
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
import os
from agents.base_agent import BaseAgent

FEW_SHOT_PROMPT = """
Database schema in the form of CREATE_TABLE statements:
{database_schema}
-- Using valid SQLite, answer the following questions for the tables provided above. 
Question: {question}
Example queries: 
{examples}
DO NOT return anything else except the SQL statement. 
"""

class FewShotAgent(BaseAgent):

    def __init__(self, llm):
        self.llm = llm
        prompt = PromptTemplate(
            input_variables=["question", "database_schema", "examples"],
            template=FEW_SHOT_PROMPT,
        )
        self.chain = LLMChain(llm=llm, prompt=prompt)

    def generate_query(self, database_schema, examples, question):
        return self.chain.run({
            'database_schema': database_schema,
            'question': question            
        })