from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from agents.zero_shot import ZeroShotAgent

FEW_SHOT_PROMPT = """
Database schema in the form of CREATE_TABLE statements:
{database_schema}

Here are a few examples, "Q"  represents the question and "A" represents the corresponding SQL-query :

Q: Who is the top spending customer and how much is the average price per single item purchased by this customer? What currency was being used?
A: SELECT T2.CustomerID, SUM(T2.Price / T2.Amount), T1.Currency FROM customers AS T1 INNER JOIN transactions_1k AS T2 ON T1.CustomerID = T2.CustomerID WHERE T2.CustomerID = ( SELECT CustomerID FROM yearmonth ORDER BY Consumption DESC LIMIT 1 ) GROUP BY T2.CustomerID, T1.Currency

Q: What elements are in the TR004_8_9 bond atoms?
A: SELECT DISTINCT T1.element FROM atom AS T1 INNER JOIN connected AS T2 ON T1.atom_id = T2.atom_id WHERE T2.bond_id = 'TR004_8_9'

Q: What is the percentage of cards whose language is French among the Story Spotlight cards?
A: SELECT CAST(SUM(CASE WHEN T2.language = 'French' THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(T1.id) FROM cards AS T1 INNER JOIN foreign_data AS T2 ON T1.uuid = T2.uuid WHERE T1.isStorySpotlight = 1

Using valid SQL, answer the following question based on the tables provided above.
It is important to use qualified column names in the SQL-query, meaning the form "SELECT table_name.column_name FROM table_name;"
Hint helps you to write the correct sqlite SQL query.
Question: {question}
Hint: {evidence}
DO NOT return anything else except the SQL query.
"""

class FewShotAgent(ZeroShotAgent):

    def __init__(self, llm):        
        self.llm = llm

        self.prompt_template = FEW_SHOT_PROMPT
        prompt = PromptTemplate(
            input_variables=["question", "database_schema", "evidence"],
            template=self.prompt_template,
        )

        self.chain = LLMChain(llm=llm, prompt=prompt)

