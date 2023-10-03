import unittest


from data_interface import DataLoader 

class TestDBUtils(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Setup: create a sample SQLite DB with one table and some data
        cls.test_db_name = "financial"
        cls.data_loader = DataLoader()
        
    
    @classmethod
    def tearDownClass(cls):
        pass

    def test_get_schema_and_sample_data(self):
        
        result = self.data_loader.get_schema_and_sample_data(self.test_db_name)
        print(result)

if __name__ == "__main__":
    unittest.main()
