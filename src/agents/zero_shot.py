from langchain.llms import OpenAI
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
import os
from agents.base_agent import BaseAgent

class ZeroShotAgent(BaseAgent):
    llm = None

    def __init__(self, llm):
        self.llm = llm
        prompt = PromptTemplate(
            input_variables=["question"],
            template="Please generate a corresponding SQL-query for this question: {question}?",
        )
        self.chain = LLMChain(llm=llm, prompt=prompt)

    def generate_query(self, question):
        return self.chain.run({
            'question': question
        })