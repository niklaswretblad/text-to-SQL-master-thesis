
import sqlite3
import os
import logging
from utils.timer import Timer
from config import load_config
from utils.utils import load_json
from collections import Counter

class Dataset:
   """
   A class to load and manage text-to-SQL datasets.
   """

   BASE_DB_PATH = None
   DATA_PATH = None
   CONFIG_PATH = os.path.abspath(
      os.path.join(os.path.dirname( __file__ ), '..', 'config/dataset_config.yaml'))

   def __init__(self):
      self.conn = None
      self.cursor = None
      self.data = []
      self.total_predicted_execution_time = 0
      self.total_gold_execution_time = 0
      self.last_predicted_execution_time = 0
      self.last_gold_execution_time = 0

      self.current_db = ""
      self.current_database_schema = ""
      self.config = None

      self.load_config()
      self.load_data()
      

   def load_data(self):
      """
      Load questions from the predefined DATA_PATH.

      Raises:
         DataLoaderError: If DATA_PATH is not defined.
      """
      if self.DATA_PATH is None:
         raise ValueError("DATA_PATH must be defined in child classes")

      data = load_json(self.DATA_PATH)      

      self.data = data


   def load_config(self):      
      self.config = load_config(self.CONFIG_PATH)

   
   def get_number_of_data_points(self):
      """
      Return the total number of questions available.
      
      Returns:
         int: The total number of questions.
      """
      return len(self.data)
   

   def get_data_point(self, index: int) -> dict:      
      """
      Retrieve a data point based on the provided index.

      Parameters:
         index (int): The index of the desired data point.

      Returns:
         dict: The retrieved data point.
      """
         
      return self.data[index]

   
   def execute_queries_and_match_data(self, sql: str, gold_sql: str, db_name: str) -> int:
      """
      Execute provided SQL queries and compare the results.

      Parameters:
         sql (str): The predicted SQL query to execute.
         gold_sql (str): The golden SQL query to compare results.
         db_name (str): The database name on which the queries will be executed.

      Returns:
         int: 1 if the results match, otherwise 0.
      """

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
         logging.info(f"Golden query execution time: {t.elapsed_time:.2f} \nSQL Query:\n" + gold_sql)
      else:
         logging.info(f"Golden query execution time: {t.elapsed_time:.2f}")
      
      self.last_gold_execution_time = t.elapsed_time
      self.total_gold_execution_time += t.elapsed_time      

      # logging.debug("Predicted data:")
      # logging.debug(set(pred_res))
      # logging.debug("Gold data:")
      # logging.debug(set(golden_res))

      equal = (Counter(pred_res) == Counter(golden_res))
      return int(equal)
   

   def execute_query(self, sql: str, db_name: str) -> int:
      """
      Execute a SQL query on a specified database and log execution time.

      Parameters:
         sql (str): The SQL query to execute.
         db_name (str): The database name on which the query will be executed.

      Returns:
         int: 1 if the query executes successfully, otherwise 0.
      """
      
      if self.current_db != db_name:
         self.load_db(db_name)
      
      try:
         with Timer() as t:
            self.cursor.execute(sql)
            pred_res = self.cursor.fetchall()
         
         if t.elapsed_time > 5:
            logging.info(f"Query execution time: {t.elapsed_time:.2f} \nSQL Query:\n" + pred_res)
         else:
            logging.info(f"Query query execution time: {t.elapsed_time:.2f}")

      except sqlite3.Error as err:
         logging.error("DataLoader.execute_query() " + str(err))
         return 0

      return 1


   def list_tables_and_columns(self, db_name: str) -> str:
      """
      List tables and columns of a specified database, logging the info.

      Parameters:
         db_name (str): The database name to list tables and columns.

      Returns:
         str: The formatted string of tables and columns information.
      """
   
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


   def get_create_statements(self, db_name: str) -> str:
      """
      Retrieve and store SQL CREATE statements for all tables in a database.

      Parameters:
         db_name (str): The name of the database to get CREATE statements.

      Returns:
         str: The SQL CREATE statements for all tables in the database.
      """
      if self.current_db != db_name:
         self.load_db(db_name)

         self.cursor.execute("SELECT sql FROM sqlite_master WHERE type='table';")
         create_statements = self.cursor.fetchall()

         self.current_database_schema = '\n'.join([statement[0] for statement in create_statements])
      
      return self.current_database_schema
   

   def get_schema_and_sample_data(self, db_name: str) -> str:
      """
      Retrieve, store, and return the schema and sample data from a database.

      Parameters:
         db_name (str): The name of the database to get schema and data.

      Returns:
         str: A formatted string containing schema and sample data.
      """
       
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


   def load_db(self, db_name: str) -> None:
      """
      Load a database into the class by connecting and setting a cursor.

      Parameters:
         db_name (str): The name of the database to load.
      """
      db_path = self.get_db_path(db_name)
      logging.debug("DB_path: " + db_path)
      self.conn = sqlite3.connect(db_path)      
      self.cursor = self.conn.cursor()
      self.current_db = db_name


   def get_db_path(self, db_name: str) -> str:
      """
      Construct and return the path to a specified database file.

      Parameters:
         db_name (str): The name of the database to find the path.

      Returns:
         str: The constructed path to the database file.

      Raises:
         ValueError: If BASE_PATH is not defined.
      """
   
      if self.BASE_DB_PATH is None:
         raise ValueError("BASE_PATH must be defined in child classes")
      return f"{self.BASE_DB_PATH}/{db_name}/{db_name}.sqlite"
   

   def get_data_path(self) -> str:
      """
      Abstract method to get the path for the data file.

      This method should be implemented in child classes.

      Raises:
         NotImplementedError: If the method is not implemented in a child class.
      """
      
      raise NotImplementedError("get_data_path() must be defined in child classes")
   

class BIRDDataset(Dataset):
   """
   Dataset class for the BIRD dataset.
   """

   DEV_DB_PATH = os.path.abspath(
      os.path.join(os.path.dirname( __file__ ), '..', 'data/BIRD/dev/dev_databases/'))
   
   TRAIN_DB_PATH = os.path.abspath(
      os.path.join(os.path.dirname( __file__ ), '..', 'data/BIRD/train/train_databases/'))

   TRAIN_DATA_PATH = os.path.abspath(
    os.path.join(os.path.dirname( __file__ ), '..', 'data/BIRD/train/train.json'))

   DEV_DATA_PATH = os.path.abspath(
    os.path.join(os.path.dirname( __file__ ), '..', 'data/BIRD/dev/dev.json'))
   
   def __init__(self):
      super().__init__()

      self.load_database_names()


   def load_data(self) -> None:
      """
      Load and filter questions specific to the BIRD dataset configurations.
      """

      if self.TRAIN_DATA_PATH is None or self.DEV_DATA_PATH is None:
         raise ValueError("QUESTIONS_PATH must be defined in child classes")

      train_data = []
      dev_data = []
      
      if self.config is not None:         
         # if self.config.bird_train_domains is not None:
            # train_data = load_json(self.TRAIN_DATA_PATH)
            # train_data = [
            #    data_point for data_point in train_data 
            #    if data_point['db_id'] in self.config.bird_train_domains
            # ]

         if self.config.bird_dev_domains is not None:
            dev_data = load_json(self.DEV_DATA_PATH)
            dev_data = [
               data_point for data_point in dev_data 
               if data_point['db_id'] in self.config.bird_dev_domains
            ]
               
            dev_data = [
               data_point for data_point in dev_data 
               if data_point['difficulty'] in self.config.bird_difficulties
            ]

      self.data = dev_data + train_data


   def load_database_names(self):
      self.dev_databases = os.listdir(self.DEV_DB_PATH)
      # self.train_databases = os.listdir(self.TRAIN_DB_PATH)

   
   def load_db(self, db_name: str) -> None:
      """
      Load a database into the class by connecting and setting a cursor.

      Parameters:
         db_name (str): The name of the database to load.
      """
      db_path = ""
      if db_name in self.dev_databases:
         db_path = f"{self.DEV_DB_PATH}/{db_name}/{db_name}.sqlite"
      else:
         db_path = f"{self.TRAIN_DB_PATH}/{db_name}/{db_name}.sqlite"
         
      self.conn = sqlite3.connect(db_path)      
      self.cursor = self.conn.cursor()
      self.current_db = db_name
   

   def get_bird_table_info(self, db_name):
      """
      Given a database name, retrieve the table schema and information 
      from the corresponding bird-bench .csv files.

      :param database_name: str, name of the database
      :return: dict, where keys are table names and values are a string
      containing the table information
      """

      description_folder_path = ""
      if db_name in self.dev_databases:
         description_folder_path = self.DEV_DB_PATH + f"/{db_name}/database_description"
      else:
         description_folder_path = self.TRAIN_DB_PATH + f"/{db_name}/database_description"
      
      if not os.path.exists(description_folder_path):
         raise FileNotFoundError(f"No such file or directory: '{description_folder_path}'")
      
      table_info = ""
      
      for filename in os.listdir(description_folder_path):
         if filename.endswith(".csv"):
            table_name = filename.rstrip(".csv")
            csv_path = os.path.join(description_folder_path, filename)
            
            with open(csv_path, mode='r', encoding='utf-8') as file:
               file_contents = file.read()                                   
            
            table_info += "Table " + table_name + "\n"
            table_info += file_contents
      
         table_info += "\n\n"

      return table_info
   

   def get_bird_db_info(self, db_path):      
      table_info = self.get_bird_table_info(db_path)

      # db_info = ""
      # for table in table_info:
      #    db_info += table_info[table]
      #    db_info += "\n\n"
      
      return table_info
   

class SpiderDataset(Dataset):
   """
   Dataset class for the Spider dataset.
   """
   
   BASE_DB_PATH = os.path.abspath(
      os.path.join(os.path.dirname( __file__ ), '..', 'data/Spider/database/'))

   TRAIN_DATA_PATH = os.path.abspath(
      os.path.join(os.path.dirname( __file__ ), '..', 'data/Spider/train_spider.json'))
   
   DEV_DATA_PATH = os.path.abspath(
      os.path.join(os.path.dirname( __file__ ), '..', 'data/Spider/dev.json'))
   
   def load_data(self) -> None:
      """
      Load and filter questions specific to the Spider dataset configurations.
      """

      if self.TRAIN_DATA_PATH is None or self.DEV_DATA_PATH is None:
         raise ValueError("DATA_PATH must be defined in child classes")

      train_data = []
      dev_data = []
      
      if self.config is not None:         
         if self.config.spider_train_domains is not None:
            train_data = load_json(self.TRAIN_DATA_PATH)
            train_data = [
               data_point for data_point in train_data 
               if data_point['db_id'] in self.config.spider_train_domains
            ]

         if self.config.spider_dev_domains is not None:
            dev_data = load_json(self.DEV_DATA_PATH)
            dev_data = [
               data_point for data_point in dev_data 
               if data_point['db_id'] in self.config.spider_dev_domains
            ]
               

      self.data = dev_data + train_data


   def get_data_point(self, index: int) -> None:
      """
      Retrieve a data point from the Spider dataset, adjusting SQL information.

      Parameters:
         index (int): The index of the desired question.

      Returns:
         dict: The selected question with modified SQL data.
      """

      data_point = self.data[index]
      data_point['SQL'] = data_point['query']
      data_point['evidence'] = ""
      del data_point['query']
      return data_point
   

   def get_train_domains(self):
      train_data = load_json(self.TRAIN_DATA_PATH)
      
      domains = set()
      for data_point in train_data:
         domains.add(data_point['db_id'])
      
      return "\n".join([domain for domain in sorted(domains)])


   def get_dev_domains(self):
      dev_data = load_json(self.DEV_DATA_PATH)
      
      domains = set()
      for data_point in dev_data:
         domains.add(data_point['db_id'])
      
      return "\n".join([domain for domain in sorted(domains)])
      

class BIRDFixedFinancialDataset(BIRDDataset):
   DEV_DATA_PATH = os.path.abspath(
      os.path.join(os.path.dirname( __file__ ), '..', 'data/BIRD/dev/financial_fixed.json'))

class BIRDExperimentalFinancialDataset(BIRDDataset):
   DEV_DATA_PATH = os.path.abspath(
      os.path.join(os.path.dirname( __file__ ), '..', 'data/BIRD/dev/financial_experimental.json'))

class BIRDFixedFinancialGoldSQL(BIRDDataset):
   DEV_DATA_PATH = os.path.abspath(
      os.path.join(os.path.dirname( __file__ ), '..', 'data/BIRD/dev/financial_gold_fixed.json'))

class BIRDCorrectedFinancialGoldAnnotated(BIRDDataset):
   DEV_DATA_PATH = os.path.abspath(
      os.path.join(os.path.dirname( __file__ ), '..', 'data/BIRD/dev/corrected_financial_annotated.json'))


DATASET_LOADERS = {
    'BIRD': BIRDDataset,
    'Spider': SpiderDataset,
    'BIRDFixedFinancial': BIRDFixedFinancialDataset,
    'BIRDExperimentalFinancial': BIRDExperimentalFinancialDataset,
    'BIRDFixedFinancialGoldSQL': BIRDFixedFinancialGoldSQL,
    'BIRDCorrectedFinancialGoldAnnotated': BIRDCorrectedFinancialGoldAnnotated
}

def get_dataset(dataset_name):
    return DATASET_LOADERS.get(dataset_name, Dataset)()