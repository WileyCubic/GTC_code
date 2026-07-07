import os
from datetime import datetime
from logging_utils.env import load_root_env
load_root_env()
import logging

from gtc_pivot_tables.config import LoggingFileConfig

from gtc_pivot_tables.logging_config import setup_logging

logging_config = LoggingFileConfig()

setup_logging(
    log_file_path=logging_config.log_file_path,
    project_name= logging_config.project_name,
    force_setup=True
)

logger = logging.getLogger(__name__)