"""
Sales ETL Common Module

This module provides shared utilities, configurations, and logging setup
for the Sales ETL pipeline system.

Modules:
    config: Database and logging configuration classes
    logging_config: Logging setup and handlers
    utils: Utility functions for data processing and phone formatting
"""

from .config import (
    MysqlDatabaseConfig,
    SqliteDatabaseConfig,
    LoggingFileCongigs
)

from .logging_config import (
    setup_logging,
    ensure_logging_configured
)

__version__ = "1.0.1"
__author__ = "D3 Design Inc"

__all__ = [
    # Configuration classes
    "MysqlDatabaseConfig",
    "SqliteDatabaseConfig", 
    "LoggingFileCongigs",
    
    # Logging functions
    "setup_logging",
    "ensure_logging_configured",
]
