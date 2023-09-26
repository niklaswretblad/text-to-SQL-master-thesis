
import sqlite3
import os
import logging
from timer import Timer

DB_BASE_PATH = os.path.abspath(
   os.path.join(os.path.dirname( __file__ ), '..', 'data/db/')
)

class DataLoader:
   current_db = ""
   conn = None
   cursor = None

   def __init__(self):
      pass

   def execute_query(self, sql, gold_sql, db_id):
      db_path = self.get_db_path(db_id)

      if self.current_db != db_id:
         self.conn = sqlite3.connect(db_path)
         self.cursor = self.conn.cursor()
         self.current_db = db_id
      
      try:
         with Timer("cursor.execute(sql) PREDICTED", {'sql: ': sql}):
            self.cursor.execute(sql)
            pred_res = self.cursor.fetchall()
      except sqlite3.Error as err:
         logging.error("DataLoader.execute_query() " + str(err))
         return 0

      with Timer("cursor.execute(sql) GOLD"):
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

   def get_create_statements(self, db_name):   
      if self.current_db != db_name:
         self.conn = sqlite3.connect(self.get_db_path(db_name))
         self.cursor = self.conn.cursor()

      self.cursor.execute("SELECT sql FROM sqlite_master WHERE type='table';")
      create_statements = self.cursor.fetchall()

      return '\n'.join([statement[0] for statement in create_statements])

   def get_db_path(self, db_name):
      return DB_BASE_PATH + '/' + db_name + '/' + db_name + '.sqlite'

    