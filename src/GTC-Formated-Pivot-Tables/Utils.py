import os
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()
import sys
from logging_utils.logging_config import setup_logging
import logging


# help find logging_utils module
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# import logging
from logging_utils.logging_config import setup_logging

setup_logging(
    log_file_path=os.getenv('ETL_Sales_to_DB_log_file'),
    project_name= 'SalesETL',
    force_setup=True
)

logger = logging.getLogger(__name__)