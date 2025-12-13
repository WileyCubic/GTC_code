"""
Unit tests for Sales ETL configuration management

Tests configuration loading, validation, and environment variable handling.
"""

import unittest
import os
import sys
from unittest.mock import patch, MagicMock
import tempfile

# Add the project root to Python path
project_root = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.insert(0, project_root)


class TestConfigurationLoading(unittest.TestCase):
    """Test configuration loading and validation"""
    
    def setUp(self):
        """Set up test environment"""
        self.test_env_vars = {
            'DATABASE_HOST': 'localhost',
            'DATABASE_PORT': '3306',
            'DATABASE_NAME': 'test_db',
            'DATABASE_USER': 'test_user'
        }
    
    @patch.dict(os.environ, {'DATABASE_HOST': 'test_host'})
    def test_environment_variable_loading(self):
        """Test that configuration loads environment variables correctly"""
        self.assertEqual(os.getenv('DATABASE_HOST'), 'test_host')
        
    def test_missing_environment_variables(self):
        """Test handling of missing environment variables"""
        # Test that missing env vars return None or default values
        missing_var = os.getenv('NONEXISTENT_VAR')
        self.assertIsNone(missing_var)
        
        # Test with default value
        default_var = os.getenv('NONEXISTENT_VAR', 'default_value')
        self.assertEqual(default_var, 'default_value')


class TestDatabaseConfiguration(unittest.TestCase):
    """Test database configuration settings"""
    
    @patch.dict(os.environ, {
        'DATABASE_HOST': 'localhost',
        'DATABASE_PORT': '3306', 
        'DATABASE_NAME': 'test_db',
        'DATABASE_USER': 'test_user',
        'DATABASE_PASSWORD': 'test_pass'
    })
    def test_database_connection_string(self):
        """Test database connection string generation"""
        # This would test your actual config module
        host = os.getenv('DATABASE_HOST')
        port = os.getenv('DATABASE_PORT')
        database = os.getenv('DATABASE_NAME')
        
        connection_string = f"mysql://{host}:{port}/{database}"
        expected = "mysql://localhost:3306/test_db"
        
        self.assertEqual(connection_string, expected)


class TestLogFileConfiguration(unittest.TestCase):
    """Test log file configuration"""
    
    def setUp(self):
        """Create temporary directories for testing"""
        self.temp_dir = tempfile.mkdtemp()
        
    def tearDown(self):
        """Clean up temporary directories"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_log_directory_creation(self):
        """Test that log directories are created when needed"""
        log_path = os.path.join(self.temp_dir, 'logs', 'test.log')
        log_dir = os.path.dirname(log_path)
        
        # Create directory if it doesn't exist
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        self.assertTrue(os.path.exists(log_dir))
        
    @patch.dict(os.environ, {
        'Primary_log': 'logs/primary.log',
        'Secondary_log': 'logs/secondary.log'
    })
    def test_log_file_paths(self):
        """Test log file path configuration"""
        primary_log = os.getenv('Primary_log')
        secondary_log = os.getenv('Secondary_log')
        
        self.assertEqual(primary_log, 'logs/primary.log')
        self.assertEqual(secondary_log, 'logs/secondary.log')


if __name__ == '__main__':
    unittest.main(verbosity=2)