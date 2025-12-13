import os
import logging
from datetime import datetime
import sys
from dotenv import load_dotenv
load_dotenv()
import pandas as pd
from common.config import logging_file_config
from common.logging_config import setup_logging


#----------------------------------------#
# LOGGING CONFIGURATION
#----------------------------------------#

log_files = logging_file_config()

 
setup_logging(
    primary_log_file_path= log_files.lineitem_analysis_log,
    secondary_log_file_path= log_files.Master_sales_ETL,
    project_name= 'LineItemAnalysisPipeline',
    force_setup=True
)

# initialize logger

logger = logging.getLogger(__name__)