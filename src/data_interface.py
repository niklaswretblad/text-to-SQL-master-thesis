
import sqlite3
import pandas as pd
import json
import os
import time

DB_BASE_PATH = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..', 'data/db/'))

class DataLoader:
   current_db = ""
   conn = None
   cursor = None

   def __init__(self):
      pass

   def execute_query(self, sql, gold_sql, db_id):
      db_path = DB_BASE_PATH + '/' + db_id + '/' + db_id + '.sqlite'

      if self.current_db != db_id:
         self.conn = sqlite3.connect(db_path)
         self.cursor = self.conn.cursor()
         self.current_db = db_id
      
      self.cursor.execute(sql)
      pred_res = self.cursor.fetchall()

      self.cursor.execute(gold_sql)
      golden_res = self.cursor.fetchall()
      
      equal = (set(pred_res) == set(golden_res))
      if equal:
         return 1
      else:
         return 0

   def list_tables_and_columns(self, database_path):
      conn = sqlite3.connect(database_path)
      cursor = conn.cursor()

      cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
      tables = cursor.fetchall()

      for table in tables:
         table_name = table[0]
         print(f"Table: {table_name}")
         
         cursor.execute(f"PRAGMA table_info({table_name});")
         columns = cursor.fetchall()
         for column in columns:
               col_name = column[1]
               col_type = column[2]
               print(f"  Column: {col_name}, Type: {col_type}")

      conn.close()

   def get_create_table_statements(self, database_path):
      pass


    