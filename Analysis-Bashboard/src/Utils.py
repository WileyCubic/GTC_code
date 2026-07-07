import os
import logging
from datetime import datetime
from logging_utils.env import load_root_env
load_root_env()
import pandas as pd


#----------------------------------------#
# LOGGING CONFIGURATION
#----------------------------------------#

from logging_utils.logging_config import setup_logging

setup_logging(
    log_file_path=os.getenv('Dashboard_log_file'),
    project_name= 'Dashboard',
    force_setup=True
)

# initialize logger

logger = logging.getLogger(__name__)