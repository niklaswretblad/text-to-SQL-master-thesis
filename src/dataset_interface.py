

class DatasetInterface:
    def execute_query(self, query):
        raise NotImplementedError("Please Implement execute_query")


    def get_questions(self):
        raise NotImplementedError("Please Implement get_questions")



