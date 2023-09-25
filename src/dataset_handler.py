
import sqlite3
import pandas as pd
import json
import os

class DatasetHandler:
    db_names = []
    current_db_name = ''
    current_db = None

    def __init__(self):
        self.db_names = self.get_db_directories()
        

    def get_db_directories(self):
        db_path = os.path.join(os.getcwd(), 'data/db')

        if not os.path.exists(db_path) or not os.path.isdir(db_path):
            return []

        return [name for name in os.listdir(db_path) if os.path.isdir(os.path.join(db_path, name))]


    def load_db(self, path):
        conn = sqlite3.connect(path)
        return conn
    
    def execute_query(self, query):
        if self.current_db is not None:
            return pd.read_sql_query(query, self.current_db)
        return 


