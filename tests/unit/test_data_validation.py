"""
Unit tests for Snowflake utilities
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.python.utils.data_validation import DataValidator, DataQualityReport
import pandas as pd


class TestDataValidator:
    """Test suite for DataValidator"""
    
    def test_validate_not_null_passes(self):
        """Test that validation passes when no nulls present"""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'email': ['a@example.com', 'b@example.com', 'c@example.com']
        })
        
        assert DataValidator.validate_not_null(df, ['id', 'name']) is True
    
    def test_validate_not_null_fails(self):
        """Test that validation fails when nulls present"""
        df = pd.DataFrame({
            'id': [1, None, 3],
            'name': ['Alice', 'Bob', 'Charlie']
        })
        
        assert DataValidator.validate_not_null(df, ['id']) is False
    
    def test_validate_row_count_within_range(self):
        """Test row count validation within acceptable range"""
        df = pd.DataFrame({'id': range(100)})
        
        assert DataValidator.validate_row_count(df, min_rows=50, max_rows=150) is True
    
    def test_validate_row_count_too_low(self):
        """Test row count validation fails when too low"""
        df = pd.DataFrame({'id': range(10)})
        
        assert DataValidator.validate_row_count(df, min_rows=50) is False
    
    def test_validate_row_count_too_high(self):
        """Test row count validation fails when too high"""
        df = pd.DataFrame({'id': range(200)})
        
        assert DataValidator.validate_row_count(df, max_rows=150) is False
    
    def test_validate_duplicates_passes(self):
        """Test duplicate validation passes when no duplicates"""
        df = pd.DataFrame({
            'customer_id': ['C001', 'C002', 'C003'],
            'email': ['a@example.com', 'b@example.com', 'c@example.com']
        })
        
        assert DataValidator.validate_duplicates(df, ['customer_id']) is True
    
    def test_validate_duplicates_fails(self):
        """Test duplicate validation fails with duplicates"""
        df = pd.DataFrame({
            'customer_id': ['C001', 'C001', 'C002'],
            'email': ['a@example.com', 'b@example.com', 'c@example.com']
        })
        
        assert DataValidator.validate_duplicates(df, ['customer_id']) is False
    
    def test_validate_data_types_passes(self):
        """Test data type validation passes with correct types"""
        df = pd.DataFrame({
            'id': pd.Series([1, 2, 3], dtype='int64'),
            'name': pd.Series(['A', 'B', 'C'], dtype='object')
        })
        
        assert DataValidator.validate_data_types(df, {'id': 'int', 'name': 'object'}) is True
    
    def test_validate_column_values_passes(self):
        """Test column value validation passes with allowed values"""
        df = pd.DataFrame({
            'status': ['active', 'active', 'inactive']
        })
        
        assert DataValidator.validate_column_values(
            df, 'status', ['active', 'inactive']
        ) is True
    
    def test_validate_column_values_fails(self):
        """Test column value validation fails with invalid values"""
        df = pd.DataFrame({
            'status': ['active', 'pending', 'invalid']
        })
        
        assert DataValidator.validate_column_values(
            df, 'status', ['active', 'inactive']
        ) is False


class TestDataQualityReport:
    """Test suite for DataQualityReport"""
    
    def test_add_check_and_generate_report(self):
        """Test adding checks and generating report"""
        report = DataQualityReport('test_table')
        report.add_check('null_check', True)
        report.add_check('duplicate_check', False, 'Found 5 duplicates')
        report.add_check('type_check', True)
        
        result = report.generate_report()
        
        assert result['table'] == 'test_table'
        assert result['total_checks'] == 3
        assert result['passed_checks'] == 2
        assert result['failed_checks'] == 1
        assert result['success_rate'] == pytest.approx(66.66, abs=0.1)
    
    def test_report_all_passed(self):
        """Test report when all checks pass"""
        report = DataQualityReport('test_table')
        report.add_check('check_1', True)
        report.add_check('check_2', True)
        
        result = report.generate_report()
        
        assert result['success_rate'] == 100.0
        assert result['failed_checks'] == 0
    
    def test_report_all_failed(self):
        """Test report when all checks fail"""
        report = DataQualityReport('test_table')
        report.add_check('check_1', False)
        report.add_check('check_2', False)
        
        result = report.generate_report()
        
        assert result['success_rate'] == 0.0
        assert result['passed_checks'] == 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
