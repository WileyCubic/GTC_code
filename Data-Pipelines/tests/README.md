# Data Pipelines Test Suite

This directory contains comprehensive unit tests for the Data Pipelines project.

## Test Structure

```
tests/
├── __init__.py                    # Test package initialization
├── run_tests.py                   # Master test runner
├── fixtures/
│   └── sample_data.py            # Test data and fixtures
├── Sales_ETL/
│   ├── common/
│   │   ├── test_config.py        # Configuration tests
│   │   ├── test_logging_config.py # Logging configuration tests  
│   │   └── test_utils.py         # Utility function tests
│   └── Pipelines/
│       ├── test_raw_orders.py    # Raw orders pipeline tests
│       └── test_line_item_analysis.py # Line item analysis tests
```

## Running Tests

### Run All Tests
```bash
python tests/run_tests.py
```

### Run Specific Test Categories
```bash
# Utils tests only
python tests/run_tests.py --category utils

# Config tests only  
python tests/run_tests.py --category config

# Pipeline tests only
python tests/run_tests.py --category pipelines
```

### Run Specific Test Module
```bash
python tests/run_tests.py --module Sales_ETL.common.test_utils
```

### Run with Coverage
```bash
# Install coverage first: pip install coverage
python tests/run_tests.py --coverage
```

### Run Individual Test Files
```bash
# From project root
python -m pytest tests/Sales_ETL/common/test_utils.py -v

# Or using unittest
python -m unittest tests.Sales_ETL.common.test_utils -v
```

## Test Categories

### 1. Common Module Tests (`Sales_ETL/common/`)

#### `test_utils.py`
- **Phone Number Formatting**: Tests all phone number formats (9, 10, 11, 12 digits)
- **DataFrame Utilities**: Tests DataFrame logging and analysis functions
- **Performance Logging**: Tests performance metric collection
- **Error Logging**: Tests error handling with context
- **Application Logging**: Tests application-specific logging setup
- **Integration Tests**: End-to-end utility function testing

#### `test_config.py`
- **Environment Variables**: Tests configuration loading from environment
- **Database Configuration**: Tests database connection settings
- **Log File Configuration**: Tests log file path configuration
- **Missing Variables**: Tests handling of missing configuration

#### `test_logging_config.py`
- **Logger Creation**: Tests multiple logger instantiation
- **File Handlers**: Tests file handler creation and rotation
- **Formatters**: Tests log message formatting
- **Dual Logging**: Tests primary/secondary log file setup
- **Log Levels**: Tests different logging levels

### 2. Pipeline Tests (`Sales_ETL/Pipelines/`)

#### `test_raw_orders.py`
- **CSV Extraction**: Tests CSV file discovery and loading
- **Data Transformation**: Tests data cleaning and formatting
- **Phone Number Processing**: Tests phone number transformation in pipeline context
- **Database Loading**: Tests DataFrame to database operations (mocked)
- **Error Handling**: Tests pipeline error scenarios
- **Integration**: End-to-end pipeline testing

#### `test_line_item_analysis.py`
- **Pattern Matching**: Tests product categorization patterns
- **Text Analysis**: Tests item name parsing and analysis
- **Sales Analytics**: Tests volume and trend analysis
- **Customer Analysis**: Tests customer behavior analysis
- **Category Analysis**: Tests product category performance
- **Integration**: Complete analysis pipeline testing

### 3. Test Fixtures (`fixtures/`)

#### `sample_data.py`
- **Sample Data Generation**: Creates realistic test data
- **Square Data**: Sample Square orders data
- **Shopify Data**: Sample Shopify orders data
- **Pattern Data**: Sample line item patterns
- **Phone Numbers**: Various phone number formats for testing
- **Mock Objects**: Mock database engines, loggers, etc.

## Test Coverage Areas

### ✅ Covered Functionality
- Phone number formatting (all formats)
- DataFrame processing utilities
- Logging configuration (single and dual file)
- CSV file processing
- Data transformation pipelines
- Pattern matching and analysis
- Error handling and recovery
- Configuration management
- Performance monitoring

### 🔄 Mocked Components
- Database connections (SQLAlchemy engines)
- File I/O operations (where appropriate)
- External API calls
- Network connections
- Email notifications

### 📊 Test Types
- **Unit Tests**: Individual function testing
- **Integration Tests**: Component interaction testing  
- **Mock Tests**: External dependency simulation
- **Performance Tests**: Timing and resource usage
- **Error Tests**: Exception handling validation

## Writing New Tests

### Test File Naming Convention
- Prefix with `test_`
- Match the module being tested: `test_<module_name>.py`

### Test Class Structure
```python
class TestModuleName(unittest.TestCase):
    """Test description"""
    
    def setUp(self):
        """Set up test fixtures before each test"""
        pass
        
    def tearDown(self):
        """Clean up after each test"""
        pass
        
    def test_specific_functionality(self):
        """Test specific functionality"""
        # Arrange
        # Act
        # Assert
        pass
```

### Using Test Fixtures
```python
from tests.fixtures.sample_data import SampleDataGenerator, MockObjects

# Generate test data
test_df = SampleDataGenerator.create_sample_square_data(10)

# Use mock objects
mock_logger = MockObjects.MockLogger()
```

## Test Environment Setup

### Required Packages
```bash
pip install pandas
pip install sqlalchemy
pip install python-dotenv
pip install coverage  # For coverage reports
pip install pytest    # Alternative test runner
```

### Environment Variables for Testing
Create a `.env.test` file:
```
DATABASE_HOST=localhost
DATABASE_PORT=3306
DATABASE_NAME=test_gtc
DATABASE_USER=test_user
DATABASE_PASSWORD=test_password
Primary_log=tests/logs/primary_test.log
Secondary_log=tests/logs/secondary_test.log
```

## Continuous Integration

### GitHub Actions Example
```yaml
name: Run Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: 3.9
      - name: Install dependencies
        run: pip install -r requirements.txt
      - name: Run tests
        run: python tests/run_tests.py --coverage
```

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure `PYTHONPATH` includes project root
2. **Missing Fixtures**: Check that fixture files are in correct location
3. **Database Tests**: Ensure test database is available or use mocks
4. **Log File Permission**: Ensure test log directories are writable

### Debug Mode
```python
# Add to test files for debugging
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Verbose Output
```bash
python tests/run_tests.py -v  # Verbose test output
python -m unittest discover -v tests/  # Alternative verbose run
```