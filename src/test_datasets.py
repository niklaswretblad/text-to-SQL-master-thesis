
import unittest
from datasets import get_dataset 

class TestDBUtils(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Setup: create a sample SQLite DB with one table and some data
        cls.test_db_name = "financial"
        cls.bird_dataset = get_dataset("BIRD")
        #cls.spider_dataset = get_dataset("Spider")
        
    
    @classmethod
    def tearDownClass(cls):
        pass


    def test_get_schema_and_sample_data(self):
        result = self.bird_dataset.get_schema_and_sample_data(self.test_db_name)
        print(result)
        
    # def test_get_spider_domains(self):
    #     dev_domains = self.dataset.get_dev_domains()
    #     train_domains = self.dataset.get_train_domains()
                
    #     print("Train domains: \n")
    #     print(train_domains)
    #     print("\n")

    #     print("Dev domains: \n")
    #     print(dev_domains)

    # def test_get_bird_domains(self):
    #     domains = self.dataset.get_dev_domains()
    #     print(domains)
    
    # def test_get_bird_table_info(self):
    #     table_info = self.bird_dataset.get_bird_table_info("california_schools")
    #     for table in table_info:
    #         print(table_info[table])

    # def test_get_bird_db_info(self):
    #     db_info = self.bird_dataset.get_bird_db_info("california_schools")
    #     print(db_info)

    # def test_question_filter(self):
    #     for data_point in self.bird_dataset.data:
    #         assert(data_point['db_id'] == 'financial')
    
    # def test_load_database_names(self):
    #     print("\n".join(self.bird_dataset.dev_databases))
    #     print("\n".join(self.bird_dataset.train_databases))

    # def test_loaded_bird_train_domains(self):        
        
    #     for data_point in self.bird_dataset.data:
    #         assert(data_point['db_id'] == 'retails')
            

if __name__ == "__main__":
    unittest.main()
