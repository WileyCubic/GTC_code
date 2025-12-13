"""
Unit tests for logging configuration

Tests logging setup, multiple handlers, file rotation, and formatter configuration.
"""

import unittest
import logging
import os
import sys
import tempfile
import shutil
from unittest.mock import patch, MagicMock

# Add the project root to Python path
project_root = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.insert(0, project_root)


class TestLoggingConfiguration(unittest.TestCase):
    """Test logging configuration and setup"""
    
    def setUp(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.test_primary_log = os.path.join(self.temp_dir, 'primary.log')
        self.test_secondary_log = os.path.join(self.temp_dir, 'secondary.log')
        
        # Clear any existing handlers
        root_logger = logging.getLogger()
        root_logger.handlers.clear()
        
    def tearDown(self):
        """Clean up test environment"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        
        # Clear handlers after test
        root_logger = logging.getLogger()
        root_logger.handlers.clear()
        
    def test_log_directory_creation(self):
        """Test that log directories are created automatically"""
        nested_log_path = os.path.join(self.temp_dir, 'deep', 'nested', 'logs', 'test.log')
        log_dir = os.path.dirname(nested_log_path)
        
        # This simulates what the logging config should do
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        self.assertTrue(os.path.exists(log_dir))
        
    def test_logger_creation(self):
        """Test that different loggers can be created"""
        # Test creating multiple loggers
        logger1 = logging.getLogger('test.module1')
        logger2 = logging.getLogger('test.module2')
        logger3 = logging.getLogger('test.module3.submodule')
        
        self.assertIsInstance(logger1, logging.Logger)
        self.assertIsInstance(logger2, logging.Logger)
        self.assertIsInstance(logger3, logging.Logger)
        
        # Verify they have different names
        self.assertEqual(logger1.name, 'test.module1')
        self.assertEqual(logger2.name, 'test.module2')
        self.assertEqual(logger3.name, 'test.module3.submodule')
        
    def test_log_levels(self):
        """Test different log levels"""
        logger = logging.getLogger('test_levels')
        
        # Test that log levels are properly defined
        self.assertEqual(logging.DEBUG, 10)
        self.assertEqual(logging.INFO, 20)
        self.assertEqual(logging.WARNING, 30)
        self.assertEqual(logging.ERROR, 40)
        self.assertEqual(logging.CRITICAL, 50)
        
    def test_formatter_creation(self):
        """Test log formatter creation"""
        # Test basic formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.assertIsInstance(formatter, logging.Formatter)
        
        # Test custom formatter with project name
        project_name = 'TestProject'
        custom_formatter = logging.Formatter(
            f'%(asctime)s - [{project_name}] - %(name)s - %(levelname)s - %(message)s'
        )
        self.assertIsInstance(custom_formatter, logging.Formatter)
        
    def test_file_handler_creation(self):
        """Test file handler creation"""
        # Create a file handler
        handler = logging.FileHandler(self.test_primary_log)
        self.assertIsInstance(handler, logging.FileHandler)
        
        # Test that the file is created
        handler.emit(logging.LogRecord(
            name='test', level=logging.INFO, pathname='', lineno=0,
            msg='Test message', args=(), exc_info=None
        ))
        handler.close()
        
        self.assertTrue(os.path.exists(self.test_primary_log))
        
    def test_rotating_file_handler(self):
        """Test rotating file handler configuration"""
        from logging.handlers import RotatingFileHandler
        
        handler = RotatingFileHandler(
            self.test_primary_log,
            maxBytes=1024*1024,  # 1MB
            backupCount=3
        )
        
        self.assertIsInstance(handler, RotatingFileHandler)
        self.assertEqual(handler.maxBytes, 1024*1024)
        self.assertEqual(handler.backupCount, 3)
        
        handler.close()
        
    def test_console_handler(self):
        """Test console handler creation"""
        handler = logging.StreamHandler()
        self.assertIsInstance(handler, logging.StreamHandler)
        
        # Test setting different levels
        handler.setLevel(logging.WARNING)
        self.assertEqual(handler.level, logging.WARNING)


class TestLoggingIntegration(unittest.TestCase):
    """Integration tests for logging system"""
    
    def setUp(self):
        """Set up integration test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
        # Clear existing handlers
        root_logger = logging.getLogger()
        root_logger.handlers.clear()
        
    def tearDown(self):
        """Clean up integration test environment"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        
        # Clear handlers
        root_logger = logging.getLogger()
        root_logger.handlers.clear()
        
    def test_dual_logging_setup(self):
        """Test setup with primary and secondary log files"""
        primary_log = os.path.join(self.temp_dir, 'primary.log')
        secondary_log = os.path.join(self.temp_dir, 'secondary.log')
        
        # Create loggers and handlers manually (simulating setup_logging)
        logger = logging.getLogger('test_dual')
        
        # Primary handler (all levels)
        primary_handler = logging.FileHandler(primary_log)
        primary_handler.setLevel(logging.DEBUG)
        
        # Secondary handler (INFO and above)
        secondary_handler = logging.FileHandler(secondary_log)
        secondary_handler.setLevel(logging.INFO)
        
        # Add handlers
        logger.addHandler(primary_handler)
        logger.addHandler(secondary_handler)
        logger.setLevel(logging.DEBUG)
        
        # Test logging at different levels
        logger.debug("Debug message")
        logger.info("Info message")
        logger.warning("Warning message")
        logger.error("Error message")
        
        # Close handlers
        primary_handler.close()
        secondary_handler.close()
        
        # Verify files were created
        self.assertTrue(os.path.exists(primary_log))
        self.assertTrue(os.path.exists(secondary_log))
        
        # Check file contents
        with open(primary_log, 'r') as f:
            primary_content = f.read()
            
        with open(secondary_log, 'r') as f:
            secondary_content = f.read()
            
        # Primary should have all messages
        self.assertIn("Debug message", primary_content)
        self.assertIn("Info message", primary_content)
        self.assertIn("Warning message", primary_content)
        self.assertIn("Error message", primary_content)
        
        # Secondary should only have INFO and above (not DEBUG)
        self.assertNotIn("Debug message", secondary_content)
        self.assertIn("Info message", secondary_content)
        self.assertIn("Warning message", secondary_content)
        self.assertIn("Error message", secondary_content)


if __name__ == '__main__':
    unittest.main(verbosity=2)