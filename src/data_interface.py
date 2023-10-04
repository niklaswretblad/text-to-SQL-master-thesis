
import sqlite3
import os
import logging
from utils.timer import Timer
from config import config
from utils.utils import load_json


DB_BASE_PATH = os.path.abspath(
   os.path.join(os.path.dirname( __file__ ), '..', 'data/db/')
)

DEV_QUESTIONS_PATH = os.path.abspath(
    os.path.join(os.path.dirname( __file__ ), '../data/dev/dev.json'))



class DataLoader:

   def __init__(self):
      self.current_db = ""
      self.conn = None
      self.cursor = None
      self.questions = None
      self.total_predicted_execution_time = 0
      self.total_gold_execution_time = 0
      self.last_predicted_execution_time = 0
      self.last_gold_execution_time = 0
      self.current_database_schema = ""

      self.load_questions()


   def load_questions(self):
      dev_questions = load_json(DEV_QUESTIONS_PATH)
      dev_questions = [question for question in dev_questions if question['db_id'] in config.domains]
      dev_questions = [question for question in dev_questions if question['difficulty'] in config.difficulties]

      self.questions = dev_questions
   
   def get_questions(self):
      return self.questions
   
   def execute_queries_and_match_data(self, sql, gold_sql, db_name):
      if self.current_db != db_name:
         self.load_db(db_name)
      
      try:
         with Timer() as t:
            self.cursor.execute(sql)
            pred_res = self.cursor.fetchall()
         
         if t.elapsed_time > 5:
            logging.info(f"Predicted query execution time: {t.elapsed_time:.2f} \nSQL Query:\n" + sql)
         else:
            logging.info(f"Predicted query execution time: {t.elapsed_time:.2f}")

         self.last_predicted_execution_time = t.elapsed_time
         self.total_predicted_execution_time += t.elapsed_time               

      except sqlite3.Error as err:
         logging.error("DataLoader.execute_queries_and_match_data() " + str(err))
         return 0

      with Timer() as t:
         self.cursor.execute(gold_sql)
         golden_res = self.cursor.fetchall()

      if t.elapsed_time > 5:
         logging.info(f"Golden query execution time: {t.elapsed_time:.2f} \nSQL Query:\n" + golden_res)
      else:
         logging.info(f"Golden query execution time: {t.elapsed_time:.2f}")
      
      self.last_gold_execution_time = t.elapsed_time
      self.total_gold_execution_time += t.elapsed_time      

      equal = (set(pred_res) == set(golden_res))
      if equal:
         return 1
      else:
         return 0
   

   def execute_query(self, sql, db_name):
      if self.current_db != db_name:
         self.load_db(db_name)
      
      try:
         with Timer() as t:
            self.cursor.execute(sql)
            pred_res = self.cursor.fetchall()
         #wandb.log({"gold_sql_execution_time": t.elapsed_time})
         
         if t.elapsed_time > 5:
            logging.info(f"Query execution time: {t.elapsed_time:.2f} \nSQL Query:\n" + pred_res)
         else:
            logging.info(f"Query query execution time: {t.elapsed_time:.2f}")

      except sqlite3.Error as err:
         logging.error("DataLoader.execute_query() " + str(err))
         return 0

      return 1


   def list_tables_and_columns(self, db_name):
      if self.current_db != db_name:
         self.load_db(db_name)

      self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
      tables = self.cursor.fetchall()

      res = ""
      for table in tables:
         table_name = table[0]
         res = res + f"Table: {table_name}\n"
         
         self.cursor.execute(f"PRAGMA table_info(\"{table_name}\");")
         columns = self.cursor.fetchall()
         for column in columns:
               col_name = column[1]
               col_type = column[2]
               res = res + f"  Column: {col_name}, Type: {col_type}\n"         

      logging.info(res)
      return res              


   def get_create_statements(self, db_name):   
      if self.current_db != db_name:
         self.load_db(db_name)

         self.cursor.execute("SELECT sql FROM sqlite_master WHERE type='table';")
         create_statements = self.cursor.fetchall()

         self.current_database_schema = '\n'.join([statement[0] for statement in create_statements])
      
      return self.current_database_schema
   

   def get_schema_and_sample_data(self, db_name):
      if self.current_db != db_name:
         self.load_db(db_name)      
      
         self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
         tables = self.cursor.fetchall()
         
         schema_and_sample_data = ""

         for table in tables:
            table = table[0]  
            self.cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}';")
            create_statement = self.cursor.fetchone()[0]
            
            schema_and_sample_data += f"{create_statement};\n\n"
            
            self.cursor.execute(f"SELECT * FROM \"{table}\" LIMIT 3;")
            rows = self.cursor.fetchall()
                     
            self.cursor.execute(f"PRAGMA table_info(\"{table}\");")
            columns = self.cursor.fetchall()
            column_names = [column[1] for column in columns]
            column_names_line = "\t".join(column_names)
            
            schema_and_sample_data += f"Three rows from {table} table:\n"
            schema_and_sample_data += f"{column_names_line}\n"

            for row in rows:
                  row_line = "\t".join([str(value) for value in row])
                  schema_and_sample_data += f"{row_line}\n"
            schema_and_sample_data += "\n"

         schema_and_sample_data += "\n"

         self.current_database_schema = schema_and_sample_data
    
      return self.current_database_schema


   def load_db(self, db_name):
      self.conn = sqlite3.connect(self.get_db_path(db_name))
      self.cursor = self.conn.cursor()
      self.current_db = db_name


   def get_db_path(self, db_name):
      return DB_BASE_PATH + '/' + db_name + '/' + db_name + '.sqlite'

    
