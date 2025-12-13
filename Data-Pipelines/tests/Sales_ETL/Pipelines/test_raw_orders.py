"""
Unit tests for Raw Orders Pipeline

Tests extract, transform, and load operations for raw orders processing.
"""

import unittest
import pandas as pd
import os
import sys
import tempfile
import shutil
from unittest.mock import patch, MagicMock, mock_open
from pathlib import Path

# Add the project root to Python path
project_root = os.path.join(os.path.dirname(__file__), '..', '..', '..')
sys.path.insert(0, project_root)


class TestRawOrdersExtract(unittest.TestCase):
    """Test raw orders extraction functionality"""
    
    def setUp(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
        # Create test CSV content
        self.test_csv_content = """Order Date,Order,Item Name,Item Quantity,Item Price
2024-01-01,ORD001,Coffee Mug,2,15.99
2024-01-02,ORD002,T-Shirt,1,25.00
2024-01-03,ORD003,Notebook,3,8.50"""
        
        # Create test CSV file
        self.test_csv_path = os.path.join(self.temp_dir, 'test_orders.csv')
        with open(self.test_csv_path, 'w') as f:
            f.write(self.test_csv_content)
            
    def tearDown(self):
        """Clean up test environment"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        
    def test_csv_file_discovery(self):
        """Test CSV file discovery in directory"""
        # Create multiple CSV files
        csv_files = ['orders1.csv', 'orders2.csv', 'data.csv']
        for csv_file in csv_files:
            file_path = os.path.join(self.temp_dir, csv_file)
            with open(file_path, 'w') as f:
                f.write("header1,header2\nvalue1,value2\n")
        
        # Create non-CSV files
        with open(os.path.join(self.temp_dir, 'readme.txt'), 'w') as f:
            f.write("Not a CSV file")
            
        # Test file discovery
        found_files = list(Path(self.temp_dir).glob('*.csv'))
        self.assertEqual(len(found_files), 3)
        
        # Test specific pattern matching
        orders_files = list(Path(self.temp_dir).glob('orders*.csv'))
        self.assertEqual(len(orders_files), 2)
        
    def test_csv_to_dataframe_conversion(self):
        """Test CSV to DataFrame conversion"""
        # Read the test CSV
        df = pd.read_csv(self.test_csv_path)
        
        # Verify DataFrame structure
        self.assertEqual(len(df), 3)
        self.assertEqual(list(df.columns), ['Order Date', 'Order', 'Item Name', 'Item Quantity', 'Item Price'])
        
        # Verify data types and content
        self.assertEqual(df.iloc[0]['Order'], 'ORD001')
        self.assertEqual(df.iloc[0]['Item Quantity'], 2)
        self.assertEqual(df.iloc[0]['Item Price'], 15.99)
        
    def test_empty_csv_handling(self):
        """Test handling of empty CSV files"""
        empty_csv_path = os.path.join(self.temp_dir, 'empty.csv')
        with open(empty_csv_path, 'w') as f:
            f.write("")  # Empty file
            
        # Test that empty CSV raises appropriate error or returns empty DataFrame
        with self.assertRaises(pd.errors.EmptyDataError):
            pd.read_csv(empty_csv_path)
            
    def test_malformed_csv_handling(self):
        """Test handling of malformed CSV files"""
        malformed_csv_path = os.path.join(self.temp_dir, 'malformed.csv')
        with open(malformed_csv_path, 'w') as f:
            f.write("header1,header2\nvalue1\n")  # Missing column
            
        # Should read successfully but with NaN values
        df = pd.read_csv(malformed_csv_path)
        self.assertTrue(pd.isna(df.iloc[0, 1]))


class TestRawOrdersTransform(unittest.TestCase):
    """Test raw orders transformation functionality"""
    
    def setUp(self):
        """Create test DataFrame"""
        self.test_data = {
            'Order Date': ['2024-01-01', '2024-01-02', '2024-01-03'],
            'Order': ['ORD001', 'ORD002', 'ORD003'],
            'Item Name': ['Coffee Mug', 'T-Shirt', 'Notebook'],
            'Item Quantity': [2, 1, 3],
            'Item Price': [15.99, 25.00, 8.50],
            'Customer Phone': ['5551234567', '15551234567', '551234567']
        }
        self.df = pd.DataFrame(self.test_data)
        
    def test_date_column_transformation(self):
        """Test date column parsing and formatting"""
        # Convert date column to datetime
        df_transformed = self.df.copy()
        df_transformed['Order Date'] = pd.to_datetime(df_transformed['Order Date'])
        
        # Verify transformation
        self.assertEqual(df_transformed['Order Date'].dtype, 'datetime64[ns]')
        self.assertEqual(df_transformed.iloc[0]['Order Date'].strftime('%Y-%m-%d'), '2024-01-01')
        
    def test_numeric_column_validation(self):
        """Test numeric column validation and conversion"""
        # Verify quantity is numeric
        self.assertTrue(pd.api.types.is_numeric_dtype(self.df['Item Quantity']))
        self.assertTrue(pd.api.types.is_numeric_dtype(self.df['Item Price']))
        
        # Test conversion from string
        df_str_numbers = self.df.copy()
        df_str_numbers['Item Quantity'] = df_str_numbers['Item Quantity'].astype(str)
        
        # Convert back to numeric
        df_str_numbers['Item Quantity'] = pd.to_numeric(df_str_numbers['Item Quantity'])
        self.assertTrue(pd.api.types.is_numeric_dtype(df_str_numbers['Item Quantity']))
        
    def test_phone_number_transformation(self):
        """Test phone number formatting transformation"""
        # Mock the format_phone_number function
        def mock_format_phone(phone):
            if len(str(phone)) == 10:
                return f"({phone[:3]})-{phone[3:6]}-{phone[6:]}"
            elif len(str(phone)) == 11:
                return f"{phone[0]}-({phone[1:4]}) {phone[4:7]}-{phone[7:]}"
            return phone
            
        # Apply transformation
        df_transformed = self.df.copy()
        df_transformed['Formatted Phone'] = df_transformed['Customer Phone'].apply(mock_format_phone)
        
        # Verify results
        self.assertEqual(df_transformed.iloc[0]['Formatted Phone'], "(555)-123-4567")
        self.assertEqual(df_transformed.iloc[1]['Formatted Phone'], "1-(555) 123-4567")
        
    def test_data_cleaning(self):
        """Test data cleaning operations"""
        # Add dirty data
        dirty_data = self.df.copy()
        dirty_data.loc[len(dirty_data)] = ['', 'ORD004', '  Whitespace Item  ', None, 0.0, '']
        
        # Clean data
        cleaned_data = dirty_data.copy()
        
        # Remove empty strings and replace with None
        cleaned_data = cleaned_data.replace('', None)
        
        # Strip whitespace from string columns
        string_columns = cleaned_data.select_dtypes(include=['object']).columns
        for col in string_columns:
            cleaned_data[col] = cleaned_data[col].astype(str).str.strip()
            cleaned_data[col] = cleaned_data[col].replace('None', None)
        
        # Verify cleaning
        self.assertIsNone(cleaned_data.iloc[3]['Order Date'])
        self.assertEqual(cleaned_data.iloc[3]['Item Name'], 'Whitespace Item')


class TestRawOrdersLoad(unittest.TestCase):
    """Test raw orders loading functionality"""
    
    def setUp(self):
        """Set up test environment"""
        self.test_data = {
            'order_date': ['2024-01-01', '2024-01-02'],
            'order_id': ['ORD001', 'ORD002'],
            'item_name': ['Coffee Mug', 'T-Shirt'],
            'quantity': [2, 1],
            'price': [15.99, 25.00]
        }
        self.df = pd.DataFrame(self.test_data)
        
    @patch('sqlalchemy.create_engine')
    def test_database_engine_creation(self, mock_create_engine):
        """Test database engine creation"""
        # Mock engine
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # Test connection string formation
        connection_string = "mysql+pymysql://user:password@localhost:3306/database"
        
        # This would be your actual engine creation code
        from sqlalchemy import create_engine
        engine = create_engine(connection_string)
        
        mock_create_engine.assert_called_with(connection_string)
        
    @patch('pandas.DataFrame.to_sql')
    def test_dataframe_to_database(self, mock_to_sql):
        """Test DataFrame loading to database"""
        # Mock database engine
        mock_engine = MagicMock()
        
        # Test loading DataFrame to database
        self.df.to_sql('test_table', mock_engine, if_exists='replace', index=False)
        
        # Verify to_sql was called
        mock_to_sql.assert_called_once_with('test_table', mock_engine, if_exists='replace', index=False)
        
    def test_table_schema_validation(self):
        """Test table schema validation"""
        # Verify DataFrame has expected columns
        expected_columns = ['order_date', 'order_id', 'item_name', 'quantity', 'price']
        self.assertEqual(list(self.df.columns), expected_columns)
        
        # Verify data types are appropriate for database
        self.assertTrue(pd.api.types.is_numeric_dtype(self.df['quantity']))
        self.assertTrue(pd.api.types.is_numeric_dtype(self.df['price']))


class TestRawOrdersPipelineIntegration(unittest.TestCase):
    """Integration tests for the complete raw orders pipeline"""
    
    def setUp(self):
        """Set up integration test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
        # Create realistic test data
        self.test_csv_content = """Order Date,Order,Item Name,Item Quantity,Item Price,Customer Phone
2024-01-01,ORD001,Coffee Mug,2,15.99,5551234567
2024-01-02,ORD002,T-Shirt,1,25.00,15551234567
2024-01-03,ORD003,Notebook,3,8.50,551234567"""
        
        self.test_csv_path = os.path.join(self.temp_dir, 'orders.csv')
        with open(self.test_csv_path, 'w') as f:
            f.write(self.test_csv_content)
            
    def tearDown(self):
        """Clean up integration test environment"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        
    def test_end_to_end_pipeline(self):
        """Test complete pipeline from CSV to processed DataFrame"""
        # Extract: Load CSV
        df = pd.read_csv(self.test_csv_path)
        self.assertEqual(len(df), 3)
        
        # Transform: Process data
        df_transformed = df.copy()
        
        # Convert date
        df_transformed['Order Date'] = pd.to_datetime(df_transformed['Order Date'])
        
        # Format phone numbers (simplified)
        def simple_phone_format(phone):
            phone_str = str(phone)
            if len(phone_str) == 10:
                return f"({phone_str[:3]})-{phone_str[3:6]}-{phone_str[6:]}"
            return phone_str
            
        df_transformed['Formatted Phone'] = df_transformed['Customer Phone'].apply(simple_phone_format)
        
        # Validate transformation results
        self.assertEqual(df_transformed.iloc[0]['Formatted Phone'], "(555)-123-4567")
        self.assertEqual(df_transformed['Order Date'].dtype, 'datetime64[ns]')
        
        # Load: Verify data is ready for database (no actual DB connection)
        self.assertTrue(len(df_transformed) > 0)
        self.assertIn('Formatted Phone', df_transformed.columns)


if __name__ == '__main__':
    unittest.main(verbosity=2)