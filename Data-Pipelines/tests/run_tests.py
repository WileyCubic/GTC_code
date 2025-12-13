"""
Master test runner for Data Pipelines project

This script runs all unit tests in the project and provides comprehensive
test coverage reporting.
"""

import unittest
import sys
import os
import importlib.util
from pathlib import Path


# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def discover_and_run_tests():
    """Discover and run all tests in the project"""
    
    # Set up test discovery
    loader = unittest.TestLoader()
    start_dir = os.path.dirname(__file__)
    
    print("=" * 70)
    print("RUNNING DATA PIPELINES UNIT TESTS")
    print("=" * 70)
    print(f"Test discovery starting from: {start_dir}")
    print(f"Project root: {project_root}")
    
    # Create test suite manually to avoid import conflicts
    suite = unittest.TestSuite()
    
    # Manually discover test files to avoid package import issues
    test_files = []
    for root, dirs, files in os.walk(start_dir):
        for file in files:
            if file.startswith('test_') and file.endswith('.py'):
                test_files.append(os.path.join(root, file))
    
    print(f"Found {len(test_files)} test files:")
    for test_file in test_files:
        print(f"  - {test_file}")
    
    # Load tests from each file individually
    for i, test_file in enumerate(test_files):
        try:
            # Get module name from file path - make it unique to avoid conflicts
            rel_path = os.path.relpath(test_file, start_dir)
            base_name = os.path.basename(test_file).replace('.py', '')
            module_name = f"test_module_{i}_{base_name}"
            
            print(f"Loading: {rel_path}")
            
            # Import the module and load tests
            spec = importlib.util.spec_from_file_location(module_name, test_file)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            # Load tests from the module
            module_suite = loader.loadTestsFromModule(module)
            suite.addTest(module_suite)
            
        except Exception as e:
            print(f"Warning: Could not load tests from {test_file}: {e}")
            import traceback
            traceback.print_exc()
    
    # Debug: Check if any tests were discovered
    test_count = suite.countTestCases()
    print(f"Discovered {test_count} test cases")
    
    # Run tests with verbose output
    runner = unittest.TextTestRunner(
        verbosity=2,
        buffer=True,  # Capture stdout/stderr during tests
        stream=sys.stdout
    )
    
    result = runner.run(suite)
    
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped) if hasattr(result, 'skipped') else 0}")
    
    if result.failures:
        print(f"\nFAILURES ({len(result.failures)}):")
        for test, traceback in result.failures:
            print(f"- {test}")
            
    if result.errors:
        print(f"\nERRORS ({len(result.errors)}):")
        for test, traceback in result.errors:
            print(f"- {test}")
    
    # Calculate success rate
    total_tests = result.testsRun
    failed_tests = len(result.failures) + len(result.errors)
    success_rate = ((total_tests - failed_tests) / total_tests * 100) if total_tests > 0 else 0
    
    print(f"\nSUCCESS RATE: {success_rate:.1f}%")
    
    return result.wasSuccessful()

def run_specific_test_module(module_name):
    """Run tests for a specific module"""
    
    print(f"Running tests for module: {module_name}")
    
    # Import the specific test module
    try:
        test_module = __import__(f'tests.{module_name}', fromlist=[module_name])
        
        # Create test suite from the module
        loader = unittest.TestLoader()
        suite = loader.loadTestsFromModule(test_module)
        
        # Run the tests
        runner = unittest.TextTestRunner(verbosity=2, buffer=True)
        result = runner.run(suite)
        
        return result.wasSuccessful()
        
    except ImportError as e:
        print(f"Error importing test module '{module_name}': {e}")
        return False

def run_test_categories():
    """Run tests by category"""
    
    categories = {
        'utils': ['Sales_ETL.common.test_utils'],
        'config': ['Sales_ETL.common.test_config', 'Sales_ETL.common.test_logging_config'],
        'pipelines': ['Sales_ETL.Pipelines.test_raw_orders', 'Sales_ETL.Pipelines.test_line_item_analysis']
    }
    
    all_passed = True
    
    for category, modules in categories.items():
        print(f"\n{'='*50}")
        print(f"RUNNING {category.upper()} TESTS")
        print(f"{'='*50}")
        
        category_passed = True
        
        for module in modules:
            print(f"\nTesting {module}...")
            try:
                # Load and run tests for this module
                loader = unittest.TestLoader()
                suite = loader.loadTestsFromName(module)
                
                runner = unittest.TextTestRunner(verbosity=1, stream=sys.stdout)
                result = runner.run(suite)
                
                if not result.wasSuccessful():
                    category_passed = False
                    all_passed = False
                    
            except Exception as e:
                print(f"Error running tests for {module}: {e}")
                category_passed = False
                all_passed = False
        
        status = "PASSED" if category_passed else "FAILED"
        print(f"\n{category.upper()} TESTS: {status}")
    
    return all_passed

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Run Data Pipelines unit tests')
    parser.add_argument('--module', '-m', help='Run tests for specific module')
    parser.add_argument('--category', '-c', help='Run tests by category (utils, config, pipelines)')
    parser.add_argument('--coverage', action='store_true', help='Run with coverage reporting (requires coverage.py)')
    
    args = parser.parse_args()
    
    try:
        if args.coverage:
            try:
                import coverage
                cov = coverage.Coverage()
                cov.start()
                print("Running tests with coverage analysis...")
            except ImportError:
                print("Coverage.py not installed. Install with: pip install coverage")
                sys.exit(1)
        
        if args.module:
            success = run_specific_test_module(args.module)
        elif args.category:
            if args.category in ['utils', 'config', 'pipelines']:
                success = run_test_categories()
            else:
                print("Invalid category. Available: utils, config, pipelines")
                sys.exit(1)
        else:
            success = discover_and_run_tests()
        
        if args.coverage:
            cov.stop()
            cov.save()
            
            print("\n" + "="*70)
            print("COVERAGE REPORT")
            print("="*70)
            cov.report()
        
        # Exit with appropriate code
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print("\nTests interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error running tests: {e}")
        sys.exit(1)