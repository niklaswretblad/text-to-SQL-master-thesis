
import unittest
from datasets import get_dataset 

class TestDBUtils(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Setup: create a sample SQLite DB with one table and some data
        cls.test_db_name = "financial"
        cls.dataset = get_dataset("BIRD")
        
    
    @classmethod
    def tearDownClass(cls):
        pass


    # def test_get_schema_and_sample_data(self):
    #     result = self.dataset.get_schema_and_sample_data(self.test_db_name)
    #     print(result)
        
    # def test_get_spider_domains(self):
    #     dev_domains = self.dataset.get_dev_domains()
    #     train_domains = self.dataset.get_train_domains()
                
    #     print("Train domains: \n")
    #     print(train_domains)
    #     print("\n")

    #     print("Dev domains: \n")
    #     print(dev_domains)

    def test_get_bird_domains(self):
        domains = self.dataset.get_dev_domains()
        print(domains)


if __name__ == "__main__":
    unittest.main()
