"""
Test fixtures and sample data for Data Pipelines testing

This module provides sample data and fixtures for testing various components
of the Data Pipelines project.
"""

import pandas as pd
from datetime import datetime, timedelta
import json


class SampleDataGenerator:
    """Generate sample data for testing"""
    
    @staticmethod
    def create_sample_square_data(num_records=10):
        """Create sample Square orders data"""
        base_date = datetime(2024, 1, 1)
        
        sample_data = {
            'Order Date': [(base_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(num_records)],
            'Order': [f'SQ{1000 + i}' for i in range(num_records)],
            'Item Name': [
                'Coffee Mug', 'T-Shirt', 'Notebook', 'Pen Set', 'Water Bottle',
                'Tote Bag', 'Keychain', 'Stickers', 'Cap', 'Jacket'
            ][:num_records],
            'Item Variation': [
                'Blue', 'Large', 'Lined', 'Black', 'Steel',
                'Canvas', 'Metal', 'Vinyl', 'Red', 'Medium'
            ][:num_records],
            'Item Quantity': [2, 1, 3, 1, 2, 1, 5, 10, 1, 1][:num_records],
            'Item Price': [15.99, 25.00, 8.50, 12.99, 18.50, 22.00, 3.99, 1.50, 24.99, 89.99][:num_records],
            'Customer Name': [f'Customer {chr(65 + i)}' for i in range(num_records)],
            'Customer Phone': [f'555123456{i}' for i in range(num_records)]
        }
        
        return pd.DataFrame(sample_data)
    
    @staticmethod
    def create_sample_shopify_data(num_records=10):
        """Create sample Shopify orders data"""
        base_date = datetime(2024, 1, 1)
        
        sample_data = {
            'Paid at': [(base_date + timedelta(days=i)).strftime('%Y-%m-%d %H:%M:%S') for i in range(num_records)],
            'Name': [f'#SP{2000 + i}' for i in range(num_records)],
            'Lineitem name': [
                'Organic Coffee Beans', 'Eco T-Shirt', 'Bamboo Notebook', 'Recycled Pen',
                'Sustainable Water Bottle', 'Hemp Tote Bag', 'Wooden Keychain',
                'Biodegradable Stickers', 'Organic Cotton Cap', 'Recycled Jacket'
            ][:num_records],
            'Lineitem quantity': [1, 2, 1, 3, 1, 2, 1, 5, 1, 1][:num_records],
            'Lineitem price': [24.99, 32.00, 12.50, 8.99, 28.50, 35.00, 6.99, 2.50, 29.99, 119.99][:num_records],
            'Billing Name': [f'Shopify Customer {i + 1}' for i in range(num_records)],
            'Shipping Country': ['USA'] * num_records,
            'Shipping Zip': [f'{10001 + i}' for i in range(num_records)]
        }
        
        return pd.DataFrame(sample_data)
    
    @staticmethod
    def create_sample_lineitem_patterns():
        """Create sample line item patterns for testing"""
        return {
            "garment_types": {
                "tshirt": ["t-shirt", "tee", "shirt", "top"],
                "pants": ["pants", "jeans", "trousers", "bottoms"],
                "dress": ["dress", "gown", "frock"],
                "jacket": ["jacket", "coat", "blazer", "hoodie"],
                "accessories": ["bag", "hat", "cap", "scarf", "belt"]
            },
            "sizes": {
                "extra_small": ["xs", "extra small", "2xs"],
                "small": ["s", "sm", "small"],
                "medium": ["m", "md", "medium", "med"],
                "large": ["l", "lg", "large"],
                "extra_large": ["xl", "xxl", "extra large", "2xl", "3xl"]
            },
            "colors": {
                "red": ["red", "crimson", "scarlet", "burgundy"],
                "blue": ["blue", "navy", "azure", "cerulean"],
                "green": ["green", "emerald", "forest", "lime"],
                "black": ["black", "charcoal", "ebony"],
                "white": ["white", "cream", "ivory", "pearl"],
                "gray": ["gray", "grey", "silver", "slate"]
            },
            "materials": {
                "cotton": ["cotton", "organic cotton", "100% cotton"],
                "polyester": ["polyester", "poly", "synthetic"],
                "wool": ["wool", "merino", "cashmere"],
                "denim": ["denim", "jean", "chambray"],
                "leather": ["leather", "genuine leather", "faux leather"]
            }
        }
    
    @staticmethod
    def create_sample_phone_numbers():
        """Create sample phone numbers for testing formatting"""
        return [
            "5551234567",      # 10 digits
            "15551234567",     # 11 digits  
            "551234567",       # 9 digits
            "015551234567",    # 12 digits
            5551234567.0,      # Float
            "555-123-4567",    # Already formatted
            "",                # Empty string
            None,              # None value
            "invalid_phone",   # Invalid text
            "123"              # Too short
        ]


class TestFixtures:
    """Test fixtures for various test scenarios"""
    
    @staticmethod
    def get_database_config():
        """Get test database configuration"""
        return {
            'host': 'localhost',
            'port': 3306,
            'database': 'test_gtc',
            'user': 'test_user',
            'password': 'test_password'
        }
    
    @staticmethod
    def get_log_file_config():
        """Get test log file configuration"""
        return {
            'primary_log': 'tests/logs/primary_test.log',
            'secondary_log': 'tests/logs/secondary_test.log', 
            'raw_orders_log': 'tests/logs/raw_orders_test.log',
            'lineitem_analysis_log': 'tests/logs/lineitem_test.log',
            'master_log': 'tests/logs/master_test.log'
        }
    
    @staticmethod
    def get_pipeline_config():
        """Get test pipeline configuration"""
        return {
            'square_input_dir': 'tests/fixtures/square_data',
            'shopify_input_dir': 'tests/fixtures/shopify_data',
            'output_dir': 'tests/output',
            'csv_pattern': '*.csv',
            'batch_size': 100,
            'max_retries': 3
        }
    
    @staticmethod
    def create_sample_csv_files(directory, data_type='square', num_files=3):
        """Create sample CSV files for testing"""
        import os
        
        if not os.path.exists(directory):
            os.makedirs(directory)
        
        for i in range(num_files):
            if data_type == 'square':
                df = SampleDataGenerator.create_sample_square_data(5)
            else:  # shopify
                df = SampleDataGenerator.create_sample_shopify_data(5)
            
            file_path = os.path.join(directory, f'{data_type}_orders_{i + 1}.csv')
            df.to_csv(file_path, index=False)
        
        return [os.path.join(directory, f'{data_type}_orders_{i + 1}.csv') for i in range(num_files)]


class MockObjects:
    """Mock objects for testing"""
    
    class MockDatabaseEngine:
        """Mock database engine for testing"""
        
        def __init__(self):
            self.connection_calls = []
            self.execute_calls = []
            
        def connect(self):
            self.connection_calls.append('connect')
            return self
            
        def execute(self, query):
            self.execute_calls.append(query)
            return self
            
        def close(self):
            self.connection_calls.append('close')
            
    class MockLogger:
        """Mock logger for testing"""
        
        def __init__(self):
            self.logs = {
                'debug': [],
                'info': [],
                'warning': [],
                'error': [],
                'critical': []
            }
            
        def debug(self, message):
            self.logs['debug'].append(message)
            
        def info(self, message):
            self.logs['info'].append(message)
            
        def warning(self, message):
            self.logs['warning'].append(message)
            
        def error(self, message, exc_info=None):
            self.logs['error'].append(message)
            
        def critical(self, message):
            self.logs['critical'].append(message)
            
        def get_all_logs(self):
            """Get all log messages"""
            all_logs = []
            for level, messages in self.logs.items():
                for message in messages:
                    all_logs.append(f"{level.upper()}: {message}")
            return all_logs


# Export commonly used fixtures
sample_square_data = SampleDataGenerator.create_sample_square_data()
sample_shopify_data = SampleDataGenerator.create_sample_shopify_data()
sample_patterns = SampleDataGenerator.create_sample_lineitem_patterns()
sample_phone_numbers = SampleDataGenerator.create_sample_phone_numbers()

__all__ = [
    'SampleDataGenerator',
    'TestFixtures', 
    'MockObjects',
    'sample_square_data',
    'sample_shopify_data',
    'sample_patterns',
    'sample_phone_numbers'
]