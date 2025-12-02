import os
import logging
from datetime import datetime
import sys
from dotenv import load_dotenv
load_dotenv()
import pandas as pd


#----------------------------------------#
# LOGGING CONFIGURATION
#----------------------------------------#

# help to find logging_utils module
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from logging_utils.logging_config import setup_logging

setup_logging(
    log_file_path=os.getenv('Dashboard_log_file'),
    project_name= 'Dashboard',
    force_setup=True
)

# initialize logger

logger = logging.getLogger(__name__)