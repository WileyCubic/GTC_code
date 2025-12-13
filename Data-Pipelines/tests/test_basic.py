"""
Simple test to verify test discovery and execution
"""

import unittest
import sys
import os

# Add project root to Python path for imports
project_root = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.insert(0, project_root)


class TestBasic(unittest.TestCase):
    """Basic tests to verify testing framework"""
    
    def test_basic_assertion(self):
        """Test that basic assertions work"""
        self.assertTrue(True)
        self.assertEqual(1 + 1, 2)
        self.assertIsNotNone("test")
        
    def test_string_operations(self):
        """Test string operations"""
        test_string = "Hello World"
        self.assertIn("World", test_string)
        self.assertEqual(test_string.lower(), "hello world")
        
    def test_list_operations(self):
        """Test list operations"""
        test_list = [1, 2, 3, 4, 5]
        self.assertEqual(len(test_list), 5)
        self.assertIn(3, test_list)
        self.assertEqual(test_list[0], 1)
        
    def test_dictionary_operations(self):
        """Test dictionary operations"""
        test_dict = {'key1': 'value1', 'key2': 'value2'}
        self.assertEqual(test_dict['key1'], 'value1')
        self.assertIn('key2', test_dict)
        self.assertEqual(len(test_dict), 2)


if __name__ == '__main__':
    unittest.main(verbosity=2)