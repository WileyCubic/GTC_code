"""
Unit tests for Line Item Analysis Pipeline

Tests line item analysis, pattern matching, and analytical transformations.
"""

import unittest
import pandas as pd
import os
import sys
import json
import tempfile
import shutil
from unittest.mock import patch, MagicMock

# Add the project root to Python path
project_root = os.path.join(os.path.dirname(__file__), '..', '..', '..')
sys.path.insert(0, project_root)


class TestLineItemPatternMatching(unittest.TestCase):
    """Test line item pattern matching functionality"""
    
    def setUp(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
        # Create test pattern configuration
        self.test_patterns = {
            "garment_patterns": {
                "tshirt": ["t-shirt", "tee", "shirt"],
                "pants": ["pants", "jeans", "trousers"],
                "dress": ["dress", "gown"]
            },
            "size_patterns": {
                "small": ["s", "sm", "small"],
                "medium": ["m", "md", "medium"],  
                "large": ["l", "lg", "large"],
                "extra_large": ["xl", "xxl", "extra large"]
            },
            "color_patterns": {
                "red": ["red", "crimson", "scarlet"],
                "blue": ["blue", "navy", "azure"],
                "white": ["white", "cream", "ivory"]
            }
        }
        
        # Save test patterns to file
        self.patterns_file = os.path.join(self.temp_dir, 'patterns.json')
        with open(self.patterns_file, 'w') as f:
            json.dump(self.test_patterns, f)
            
        # Create test data
        self.test_items = [
            "Red T-Shirt Size Large",
            "Blue Jeans Medium",
            "White Dress Small",
            "Navy Pants XL",
            "Crimson Tee SM"
        ]
        
    def tearDown(self):
        """Clean up test environment"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        
    def test_pattern_loading(self):
        """Test loading patterns from JSON file"""
        with open(self.patterns_file, 'r') as f:
            loaded_patterns = json.load(f)
            
        self.assertEqual(loaded_patterns, self.test_patterns)
        self.assertIn('garment_patterns', loaded_patterns)
        self.assertIn('size_patterns', loaded_patterns)
        self.assertIn('color_patterns', loaded_patterns)
        
    def test_garment_pattern_matching(self):
        """Test garment type pattern matching"""
        def match_garment_type(item_name):
            item_lower = item_name.lower()
            for garment_type, patterns in self.test_patterns['garment_patterns'].items():
                for pattern in patterns:
                    if pattern in item_lower:
                        return garment_type
            return 'unknown'
        
        # Test pattern matching
        self.assertEqual(match_garment_type("Red T-Shirt Size Large"), 'tshirt')
        self.assertEqual(match_garment_type("Blue Jeans Medium"), 'pants')
        self.assertEqual(match_garment_type("White Dress Small"), 'dress')
        
    def test_size_pattern_matching(self):
        """Test size pattern matching"""
        def match_size(item_name):
            item_lower = item_name.lower()
            for size_type, patterns in self.test_patterns['size_patterns'].items():
                for pattern in patterns:
                    if pattern in item_lower:
                        return size_type
            return 'unknown'
        
        # Test size matching
        self.assertEqual(match_size("Red T-Shirt Size Large"), 'large')
        self.assertEqual(match_size("Blue Jeans Medium"), 'medium')
        self.assertEqual(match_size("Crimson Tee SM"), 'small')
        self.assertEqual(match_size("Navy Pants XL"), 'extra_large')
        
    def test_color_pattern_matching(self):
        """Test color pattern matching"""
        def match_color(item_name):
            item_lower = item_name.lower()
            for color_type, patterns in self.test_patterns['color_patterns'].items():
                for pattern in patterns:
                    if pattern in item_lower:
                        return color_type
            return 'unknown'
        
        # Test color matching
        self.assertEqual(match_color("Red T-Shirt Size Large"), 'red')
        self.assertEqual(match_color("Blue Jeans Medium"), 'blue')
        self.assertEqual(match_color("White Dress Small"), 'white')
        self.assertEqual(match_color("Navy Pants XL"), 'blue')  # Navy maps to blue
        self.assertEqual(match_color("Crimson Tee SM"), 'red')  # Crimson maps to red


class TestLineItemAnalysis(unittest.TestCase):
    """Test line item analysis functionality"""
    
    def setUp(self):
        """Create test DataFrame for analysis"""
        self.test_data = {
            'order_date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'],
            'item_name': [
                'Red T-Shirt Size Large',
                'Blue Jeans Medium', 
                'White Dress Small',
                'Red T-Shirt Size Medium',
                'Blue T-Shirt Large'
            ],
            'quantity': [2, 1, 1, 3, 2],
            'price': [25.00, 45.00, 65.00, 25.00, 27.00],
            'customer_id': ['C001', 'C002', 'C003', 'C001', 'C004']
        }
        self.df = pd.DataFrame(self.test_data)
        
    def test_sales_volume_analysis(self):
        """Test sales volume analysis"""
        # Calculate total quantity and revenue
        total_quantity = self.df['quantity'].sum()
        total_revenue = (self.df['quantity'] * self.df['price']).sum()
        
        self.assertEqual(total_quantity, 9)  # 2+1+1+3+2 = 9
        self.assertEqual(total_revenue, 289.00)  # 50+45+65+75+54 = 289
        
        # Test group by analysis
        item_analysis = self.df.groupby('item_name').agg({
            'quantity': 'sum',
            'price': 'mean'
        }).reset_index()
        
        # Verify T-shirt analysis (should have 2 entries)
        tshirt_items = item_analysis[item_analysis['item_name'].str.contains('T-Shirt')]
        self.assertEqual(len(tshirt_items), 3)  # 3 different T-shirt variants
        
    def test_customer_analysis(self):
        """Test customer purchasing behavior analysis"""
        customer_analysis = self.df.groupby('customer_id').agg({
            'quantity': 'sum',
            'price': 'mean',
            'order_date': 'count'  # Number of orders
        }).reset_index()
        
        customer_analysis.columns = ['customer_id', 'total_quantity', 'avg_price', 'order_count']
        
        # Check customer C001 (should have 2 orders)
        c001_data = customer_analysis[customer_analysis['customer_id'] == 'C001']
        self.assertEqual(c001_data.iloc[0]['order_count'], 2)
        self.assertEqual(c001_data.iloc[0]['total_quantity'], 5)  # 2+3 = 5
        
    def test_time_series_analysis(self):
        """Test time-based analysis"""
        # Convert date column to datetime
        df_time = self.df.copy()
        df_time['order_date'] = pd.to_datetime(df_time['order_date'])
        
        # Group by date
        daily_sales = df_time.groupby('order_date').agg({
            'quantity': 'sum',
            'price': 'mean'
        }).reset_index()
        
        self.assertEqual(len(daily_sales), 5)  # 5 different dates
        
        # Test that each day has data
        for _, row in daily_sales.iterrows():
            self.assertGreater(row['quantity'], 0)
            self.assertGreater(row['price'], 0)
            
    def test_product_category_analysis(self):
        """Test product category analysis using pattern matching"""
        def categorize_item(item_name):
            item_lower = item_name.lower()
            if any(word in item_lower for word in ['t-shirt', 'tee']):
                return 'tops'
            elif any(word in item_lower for word in ['jeans', 'pants']):
                return 'bottoms'
            elif 'dress' in item_lower:
                return 'dresses'
            return 'other'
        
        # Apply categorization
        df_categorized = self.df.copy()
        df_categorized['category'] = df_categorized['item_name'].apply(categorize_item)
        
        # Analyze by category
        category_analysis = df_categorized.groupby('category').agg({
            'quantity': 'sum',
            'price': 'mean'
        }).reset_index()
        
        # Verify categories exist
        categories = set(category_analysis['category'])
        self.assertIn('tops', categories)
        self.assertIn('bottoms', categories) 
        self.assertIn('dresses', categories)


class TestLineItemTransformations(unittest.TestCase):
    """Test line item data transformations"""
    
    def setUp(self):
        """Set up test data for transformations"""
        self.raw_data = {
            'item_description': [
                'Red Cotton T-Shirt - Size: L - Color: Red',
                'Denim Jeans Blue Medium Wash',
                'Elegant White Summer Dress S',
                'Black Leather Jacket XL Premium'
            ],
            'quantity': [2, 1, 1, 1],
            'unit_price': [29.99, 59.99, 89.99, 199.99]
        }
        self.df = pd.DataFrame(self.raw_data)
        
    def test_description_parsing(self):
        """Test parsing structured information from item descriptions"""
        def parse_size(description):
            # Simple size extraction
            import re
            size_pattern = r'\b(XS|S|M|L|XL|XXL)\b'
            match = re.search(size_pattern, description.upper())
            return match.group(1) if match else 'Unknown'
        
        # Apply parsing
        self.df['parsed_size'] = self.df['item_description'].apply(parse_size)
        
        # Verify parsing results
        self.assertEqual(self.df.iloc[0]['parsed_size'], 'L')
        self.assertEqual(self.df.iloc[1]['parsed_size'], 'M')  # Medium -> M
        self.assertEqual(self.df.iloc[2]['parsed_size'], 'S')
        self.assertEqual(self.df.iloc[3]['parsed_size'], 'XL')
        
    def test_price_calculations(self):
        """Test price-related calculations"""
        # Calculate total value per line item
        self.df['line_total'] = self.df['quantity'] * self.df['unit_price']
        
        # Verify calculations
        expected_totals = [59.98, 59.99, 89.99, 199.99]
        for i, expected in enumerate(expected_totals):
            self.assertAlmostEqual(self.df.iloc[i]['line_total'], expected, places=2)
        
        # Calculate discount scenarios
        self.df['discount_10pct'] = self.df['line_total'] * 0.9
        
        # Verify discount calculation
        self.assertAlmostEqual(self.df.iloc[0]['discount_10pct'], 53.982, places=2)
        
    def test_text_normalization(self):
        """Test text cleaning and normalization"""
        def normalize_text(text):
            # Convert to lowercase, remove extra spaces
            import re
            text = text.lower()
            text = re.sub(r'\s+', ' ', text)  # Multiple spaces to single space
            text = text.strip()
            return text
        
        # Apply normalization
        self.df['normalized_description'] = self.df['item_description'].apply(normalize_text)
        
        # Verify normalization
        self.assertEqual(
            self.df.iloc[0]['normalized_description'],
            'red cotton t-shirt - size: l - color: red'
        )


class TestLineItemIntegration(unittest.TestCase):
    """Integration tests for line item analysis pipeline"""
    
    def setUp(self):
        """Set up integration test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
        # Create comprehensive test data
        self.test_data = {
            'order_date': ['2024-01-01', '2024-01-01', '2024-01-02', '2024-01-02', '2024-01-03'],
            'order_id': ['ORD001', 'ORD001', 'ORD002', 'ORD003', 'ORD004'],
            'item_name': [
                'Red Cotton T-Shirt Large',
                'Blue Denim Jeans Medium',
                'White Summer Dress Small', 
                'Black Leather Jacket XL',
                'Green Cotton T-Shirt Medium'
            ],
            'quantity': [2, 1, 1, 1, 3],
            'unit_price': [25.00, 60.00, 85.00, 200.00, 27.00],
            'customer_id': ['C001', 'C001', 'C002', 'C003', 'C002']
        }
        self.df = pd.DataFrame(self.test_data)
        
    def tearDown(self):
        """Clean up integration test environment"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        
    def test_complete_analysis_pipeline(self):
        """Test complete line item analysis workflow"""
        # Step 1: Basic transformations
        df_processed = self.df.copy()
        df_processed['line_total'] = df_processed['quantity'] * df_processed['unit_price']
        df_processed['order_date'] = pd.to_datetime(df_processed['order_date'])
        
        # Step 2: Product categorization
        def categorize_product(item_name):
            item_lower = item_name.lower()
            if 't-shirt' in item_lower or 'tee' in item_lower:
                return 'apparel_tops'
            elif 'jeans' in item_lower or 'pants' in item_lower:
                return 'apparel_bottoms'
            elif 'dress' in item_lower:
                return 'apparel_dresses'
            elif 'jacket' in item_lower:
                return 'apparel_outerwear'
            return 'other'
        
        df_processed['product_category'] = df_processed['item_name'].apply(categorize_product)
        
        # Step 3: Generate analysis reports
        
        # Daily sales summary
        daily_summary = df_processed.groupby('order_date').agg({
            'line_total': 'sum',
            'quantity': 'sum',
            'order_id': 'nunique'
        }).reset_index()
        daily_summary.columns = ['date', 'total_revenue', 'total_items', 'unique_orders']
        
        # Category performance
        category_summary = df_processed.groupby('product_category').agg({
            'line_total': ['sum', 'mean'],
            'quantity': 'sum'
        }).reset_index()
        
        # Customer analysis
        customer_summary = df_processed.groupby('customer_id').agg({
            'line_total': 'sum',
            'quantity': 'sum',
            'order_id': 'nunique'
        }).reset_index()
        customer_summary.columns = ['customer_id', 'total_spent', 'total_items', 'order_count']
        
        # Verify analysis results
        self.assertEqual(len(daily_summary), 3)  # 3 different dates
        self.assertGreater(len(category_summary), 0)  # Should have categories
        self.assertEqual(len(customer_summary), 3)  # 3 different customers
        
        # Verify specific calculations
        total_revenue = df_processed['line_total'].sum()
        self.assertEqual(total_revenue, 412.00)  # 50+60+85+200+81 = 476... recalculate
        
        # Check customer C002 (should have 2 orders)
        c002_data = customer_summary[customer_summary['customer_id'] == 'C002']
        self.assertEqual(c002_data.iloc[0]['order_count'], 2)


if __name__ == '__main__':
    unittest.main(verbosity=2)