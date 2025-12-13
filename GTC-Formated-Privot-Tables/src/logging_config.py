import os
import logging
import logging.handlers
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

# Configure logging with better practices

def setup_logging(log_file_path=None, project_name=None, force_setup=False):
    """
    Setup logging configuration - only if not already configured
    
    Args:
        log_file_path: Custom log file path
        project_name: Project name for logger identification
        force_setup: Force reconfiguration even if already setup
    """
    # Check if logging is already configured
    root_logger = logging.getLogger()
    if root_logger.handlers and not force_setup:
        return  # Already configured, don't setup again
    
    # Determine log file path
    if not log_file_path:
        log_file_path = os.getenv('Backup_log')
    
    # Clear existing handlers if force_setup
    if force_setup:
        root_logger.handlers.clear()
    
    # File handler
    file_handler = logging.handlers.RotatingFileHandler(
        log_file_path, 
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_formatter = logging.Formatter(
        f'%(asctime)s - {project_name or "Unknown"} - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    file_handler.setFormatter(file_formatter)
    
    # Console handler (only warnings and above)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    console_formatter = logging.Formatter('%(levelname)s: %(message)s')
    console_handler.setFormatter(console_formatter)
    
    # Add handlers to root logger
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    root_logger.setLevel(logging.DEBUG)

def ensure_logging_configured():
    """Ensure logging is configured with basic setup if not already done"""
    if not logging.getLogger().handlers:
        setup_logging()

# Create logger for this module
logger = logging.getLogger(__name__)

logger.info("Logging configuration initialized.")