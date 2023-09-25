from langchain.llms import OpenAI
import os

api_key = os.environ.get('OPENAI_API_KEY')
    if api_key is None:
        raise ValueError("OPENAI_API_KEY environment variable is not set.")

llm = OpenAI(openai_api_key=api_key)