import logging
import time

class Timer:
    def __init__(self, function_name, optional_info={}):
        self.function_name = function_name
        self.optional_info = optional_info
        self.elapsed_time = None

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.elapsed_time = time.time() - self.start_time
                
        optional_info_str = ' '.join(f"{key}:{value}" for key, value in self.optional_info.items())

        logging.info(f"{self.function_name} took {self.elapsed_time:.2f} seconds to execute." + optional_info_str)


class SQLTimer(Timer):
    def __init__(self, function_name, optional_info={}):
        self.function_name = function_name
        self.optional_info = optional_info
        self.elapsed_time = None

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.elapsed_time = time.time() - self.start_time

        optional_info_str = ''
        if self.elapsed_time > 5:
            optional_info_str = ' '.join(f"{key}:{value}" for key, value in self.optional_info.items())

        logging.info(f"{self.function_name} took {self.elapsed_time:.2f} seconds to execute." + "\n" + optional_info_str)
