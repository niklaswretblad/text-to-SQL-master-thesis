import logging
import time

class Timer:
    def __init__(self, function_name):
        self.function_name = function_name

    def __enter__(self):
        self.start_time = time.time()

    def __exit__(self, exc_type, exc_value, traceback):
        elapsed_time = time.time() - self.start_time
        logging.info(f"{self.function_name} took {elapsed_time:.2f} seconds to execute.")