import os
import logging
from datetime import datetime
import sys
from dotenv import load_dotenv
load_dotenv()
import pandas as pd
from Sales_ETL.common.config import logging_file_config
from Sales_ETL.common.logging_config import setup_logging


#----------------------------------------#
# LOGGING CONFIGURATION
#----------------------------------------#

log_files = logging_file_config()

setup_logging(
    primary_log_file_path = log_files.raw_orders_log,
    secondary_log_file_path= log_files.Master_sales_ETL,
    project_name= 'RawOrdersPipeline',
    force_setup=True
)


# initialize logger

logger = logging.getLogger(__name__)
