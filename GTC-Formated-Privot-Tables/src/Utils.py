import os
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()
import sys
import logging

from config import LoggingFileConfig

from logging_config import setup_logging

logging_config = LoggingFileConfig()

setup_logging(
    log_file_path=logging_config.log_file_path,
    project_name= logging_config.project_name,
    force_setup=True
)

logger = logging.getLogger(__name__)