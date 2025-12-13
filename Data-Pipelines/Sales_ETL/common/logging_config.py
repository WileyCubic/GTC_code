import os
import logging
import logging.handlers
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

# Configure logging with better practices

def setup_logging(primary_log_file_path=None, secondary_log_file_path=None, project_name=None, force_setup=False):
    """
    Setup logging configuration - writes to primary and secondary log files
    
    Args:
        primary_log_file_path: Primary log file path
        secondary_log_file_path: Secondary log file path (optional)
        project_name: Project name for logger identification
        force_setup: Force reconfiguration even if already setup
    """
    # Check if logging is already configured
    root_logger = logging.getLogger()
    if root_logger.handlers and not force_setup:
        return  # Already configured, don't setup again
    
    # Determine primary log file path
    if not primary_log_file_path:
        primary_log_file_path = os.getenv('Primary_log', 'logs/primary.log')
    
    # Determine secondary log file path  
    if not secondary_log_file_path:
        secondary_log_file_path = os.getenv('Secondary_log', 'logs/secondary.log')
    
    # Ensure log directories exist
    for log_path in [primary_log_file_path, secondary_log_file_path]:
        log_dir = os.path.dirname(log_path)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
    
    # Clear existing handlers if force_setup
    if force_setup:
        root_logger.handlers.clear()
    
    # Primary file handler - detailed logging (all levels)
    primary_file_handler = logging.handlers.RotatingFileHandler(
        primary_log_file_path, 
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    primary_formatter = logging.Formatter(
        f'%(asctime)s - [{project_name or "Unknown"}] - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    primary_file_handler.setFormatter(primary_formatter)
    primary_file_handler.setLevel(logging.DEBUG)  # Capture everything
    
    # Secondary file handler - summary logging (INFO and above)
    secondary_file_handler = logging.handlers.RotatingFileHandler(
        secondary_log_file_path,
        maxBytes=5*1024*1024,   # 5MB
        backupCount=3
    )
    secondary_formatter = logging.Formatter(
        f'%(asctime)s - [{project_name or "Unknown"}] - %(levelname)s - %(message)s'
    )
    secondary_file_handler.setFormatter(secondary_formatter)
    secondary_file_handler.setLevel(logging.INFO)  # Only INFO and above
    
    # Console handler (only warnings and above)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    console_formatter = logging.Formatter('[%(name)s] %(levelname)s: %(message)s')
    console_handler.setFormatter(console_formatter)
    
    # Add all handlers to root logger
    root_logger.addHandler(primary_file_handler)
    root_logger.addHandler(secondary_file_handler)
    root_logger.addHandler(console_handler)
    root_logger.setLevel(logging.DEBUG)

def ensure_logging_configured():
    """Ensure logging is configured with basic setup if not already done"""
    if not logging.getLogger().handlers:
        setup_logging()

# Create logger for this module
logger = logging.getLogger(__name__)

logger.info("Dual logging configuration initialized - primary and secondary log files active")

logger.info("Logging configuration initialized.")