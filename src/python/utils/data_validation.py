"""
Data validation utilities for Snowflake pipeline
"""

import logging
from typing import Any, List, Dict, Optional
import pandas as pd

logger = logging.getLogger(__name__)


class DataValidator:
    """Validates data quality during ETL processes"""

    @staticmethod
    def validate_not_null(df: pd.DataFrame, columns: List[str]) -> bool:
        """
        Validate that specified columns have no null values

        Args:
            df: DataFrame to validate
            columns: List of column names to check

        Returns:
            True if validation passes, False otherwise
        """
        for col in columns:
            if col in df.columns and df[col].isnull().any():
                logger.warning(f"Column '{col}' contains null values: {df[col].isnull().sum()}")
                return False
        return True

    @staticmethod
    def validate_data_types(df: pd.DataFrame, expected_types: Dict[str, str]) -> bool:
        """
        Validate that columns match expected data types

        Args:
            df: DataFrame to validate
            expected_types: Dict mapping column names to expected types

        Returns:
            True if validation passes, False otherwise
        """
        for col, expected_type in expected_types.items():
            if col not in df.columns:
                logger.warning(f"Expected column '{col}' not found")
                return False

            if not str(df[col].dtype).startswith(expected_type):
                logger.warning(f"Column '{col}' has type {df[col].dtype}, expected {expected_type}")
                return False

        return True

    @staticmethod
    def validate_row_count(
        df: pd.DataFrame, min_rows: int = 0, max_rows: Optional[int] = None
    ) -> bool:
        """
        Validate row count is within acceptable range

        Args:
            df: DataFrame to validate
            min_rows: Minimum acceptable rows
            max_rows: Maximum acceptable rows (None for unlimited)

        Returns:
            True if validation passes, False otherwise
        """
        row_count = len(df)

        if row_count < min_rows:
            logger.warning(f"Row count {row_count} is below minimum {min_rows}")
            return False

        if max_rows and row_count > max_rows:
            logger.warning(f"Row count {row_count} exceeds maximum {max_rows}")
            return False

        return True

    @staticmethod
    def validate_duplicates(df: pd.DataFrame, subset: List[str]) -> bool:
        """
        Validate that specified columns have no duplicate values

        Args:
            df: DataFrame to validate
            subset: List of column names to check for duplicates

        Returns:
            True if no duplicates, False otherwise
        """
        duplicates = df.duplicated(subset=subset)

        if duplicates.any():
            logger.warning(f"Found {duplicates.sum()} duplicate rows")
            return False

        return True

    @staticmethod
    def validate_column_values(df: pd.DataFrame, col: str, allowed_values: List[Any]) -> bool:
        """
        Validate that column values are within allowed set

        Args:
            df: DataFrame to validate
            col: Column name to validate
            allowed_values: List of allowed values

        Returns:
            True if all values are allowed, False otherwise
        """
        invalid_values = df[~df[col].isin(allowed_values)]

        if not invalid_values.empty:
            logger.warning(
                f"Column '{col}' contains invalid values: {invalid_values[col].unique()}"
            )
            return False

        return True


class DataQualityReport:
    """Generates data quality reports"""

    def __init__(self, table_name: str):
        self.table_name = table_name
        self.checks = []

    def add_check(self, check_name: str, passed: bool, details: str = "") -> None:
        """
        Add a quality check result

        Args:
            check_name: Name of the quality check
            passed: Whether the check passed
            details: Additional details about the check
        """
        self.checks.append({"check": check_name, "passed": passed, "details": details})

    def generate_report(self) -> Dict[str, Any]:
        """
        Generate quality report summary

        Returns:
            Dictionary containing report data
        """
        total_checks = len(self.checks)
        passed_checks = sum(1 for c in self.checks if c["passed"])

        report = {
            "table": self.table_name,
            "total_checks": total_checks,
            "passed_checks": passed_checks,
            "failed_checks": total_checks - passed_checks,
            "success_rate": (passed_checks / total_checks * 100) if total_checks > 0 else 0,
            "details": self.checks,
        }

        logger.info(
            f"Quality Report for {self.table_name}: {passed_checks}/{total_checks} checks passed"
        )
        return report
