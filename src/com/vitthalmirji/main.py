# Main file

# Create objects & invoke methods required for your ETL process
import datetime
import logging

from utils.Utilities import init_logging

if __name__ == '__main__':
    init_logging(log_time_stamp=datetime.datetime.now().isoformat().__str__())
    logging.debug("Hello")
