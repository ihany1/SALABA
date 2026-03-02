"""
Validation utilities for data processing
Ensures data quality and required column presence
"""

import pandas as pd
from typing import List, Dict, Tuple


class DataValidator:
    """Validates uploaded data structure and content"""

    # Required columns for sales data
    REQUIRED_COLUMNS = [
        'Inv#',  # Invoice Number
        'Date',  # Transaction Date
        'Customer',  # Customer Name/ID
        'Sales Rep',  # Sales Representative
        'Production Line',  # Product Line
        'Total Amount'  # Line item total
    ]

    # Optional but recommended columns
    OPTIONAL_COLUMNS = [
        'Item',  # Product Item
        'Quantity',  # Quantity
        'Unit Price',  # Price per unit
        'Group Item',  # Item grouping
        'Size'  # Product size
    ]

    @staticmethod
    def validate_columns(df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """
        Check if all required columns exist

        Args:
            df: DataFrame to validate

        Returns:
            Tuple of (is_valid, list of missing columns)
        """
        missing_columns = []

        for col in DataValidator.REQUIRED_COLUMNS:
            if col not in df.columns:
                missing_columns.append(col)

        is_valid = len(missing_columns) == 0
        return is_valid, missing_columns

    @staticmethod
    def validate_data_types(df: pd.DataFrame) -> Tuple[bool, Dict[str, str]]:
        """
        Validate and report data type issues

        Args:
            df: DataFrame to validate

        Returns:
            Tuple of (is_valid, dict of column: error message)
        """
        errors = {}

        # Check Date column
        if 'Date' in df.columns:
            try:
                pd.to_datetime(df['Date'])
            except Exception as e:
                errors['Date'] = f"Cannot convert to datetime: {str(e)}"

        # Check Total Amount column
        if 'Total Amount' in df.columns:
            try:
                pd.to_numeric(df['Total Amount'])
            except Exception as e:
                errors['Total Amount'] = f"Cannot convert to numeric: {str(e)}"

        # Check Quantity if present
        if 'Quantity' in df.columns:
            try:
                pd.to_numeric(df['Quantity'])
            except Exception as e:
                errors['Quantity'] = f"Cannot convert to numeric: {str(e)}"

        # Check Unit Price if present
        if 'Unit Price' in df.columns:
            try:
                pd.to_numeric(df['Unit Price'])
            except Exception as e:
                errors['Unit Price'] = f"Cannot convert to numeric: {str(e)}"

        is_valid = len(errors) == 0
        return is_valid, errors

    @staticmethod
    def validate_invoice_structure(df: pd.DataFrame) -> Tuple[bool, str]:
        """
        Validate invoice grouping logic

        Args:
            df: DataFrame to validate

        Returns:
            Tuple of (is_valid, error message if any)
        """
        if 'Inv#' not in df.columns:
            return False, "Invoice Number column (Inv#) is missing"

        # Check for null invoice numbers (allow a small percentage)
        null_count = df['Inv#'].isnull().sum()
        total_rows = len(df)
        null_percentage = (null_count / total_rows * 100) if total_rows > 0 else 0

        # Allow up to 1% missing invoice numbers
        if null_percentage > 1.0:
            return False, f"Found {null_count} rows ({null_percentage:.2f}%) with missing Invoice Number"

        # If there are some nulls but within tolerance, just filter them out
        if null_count > 0:
            return True, f"Warning: {null_count} rows with missing Invoice Number will be skipped"

        return True, ""

    @staticmethod
    def get_validation_report(df: pd.DataFrame) -> Dict:
        """
        Generate comprehensive validation report

        Args:
            df: DataFrame to validate

        Returns:
            Dictionary with validation results
        """
        report = {
            'is_valid': True,
            'row_count': len(df),
            'column_count': len(df.columns),
            'columns': list(df.columns),
            'errors': [],
            'warnings': []
        }

        # Check required columns
        cols_valid, missing_cols = DataValidator.validate_columns(df)
        if not cols_valid:
            report['is_valid'] = False
            report['errors'].append({
                'type': 'missing_columns',
                'message': f"Missing required columns: {', '.join(missing_cols)}"
            })

        # Check data types
        types_valid, type_errors = DataValidator.validate_data_types(df)
        if not types_valid:
            report['is_valid'] = False
            for col, error in type_errors.items():
                report['errors'].append({
                    'type': 'data_type',
                    'column': col,
                    'message': error
                })

        # Check invoice structure
        inv_valid, inv_error = DataValidator.validate_invoice_structure(df)
        if not inv_valid:
            report['is_valid'] = False
            report['errors'].append({
                'type': 'invoice_structure',
                'message': inv_error
            })

        # Check for optional columns
        for col in DataValidator.OPTIONAL_COLUMNS:
            if col not in df.columns:
                report['warnings'].append({
                    'type': 'optional_column',
                    'message': f"Optional column '{col}' not found. Some features may be limited."
                })

        return report


def allowed_file(filename: str, allowed_extensions: set) -> bool:
    """
    Check if file extension is allowed

    Args:
        filename: Name of the file
        allowed_extensions: Set of allowed extensions

    Returns:
        True if file extension is allowed
    """
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in allowed_extensions
