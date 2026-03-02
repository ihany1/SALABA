"""
Data Ingestion Service
Handles Excel upload, validation, normalization, and processing
CRITICAL: Maintains invoice-level awareness throughout
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Tuple, Optional
from datetime import datetime
import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))

from utils.validators import DataValidator
from utils.helpers import parse_date_column, identify_returns


class DataIngestionService:
    """
    Service for ingesting and processing sales data from Excel files
    Ensures invoice-level aggregation correctness
    """

    def __init__(self):
        """Initialize the data ingestion service"""
        self.raw_data: Optional[pd.DataFrame] = None
        self.processed_data: Optional[pd.DataFrame] = None
        self.invoice_data: Optional[pd.DataFrame] = None
        self.metadata: Dict = {}
        self.validator = DataValidator()

    def upload_file(self, file_path: str) -> Tuple[bool, Dict]:
        """
        Upload and process Excel file

        Args:
            file_path: Path to Excel file

        Returns:
            Tuple of (success, result_dict)
        """
        try:
            # Read Excel file
            self.raw_data = self._read_excel(file_path)

            # Validate data structure
            validation_report = self.validator.get_validation_report(self.raw_data)

            if not validation_report['is_valid']:
                return False, {
                    'error': 'Validation failed',
                    'validation_report': validation_report
                }

            # Process and normalize data
            self.processed_data = self._process_data(self.raw_data)

            # Create invoice-level aggregation
            self.invoice_data = self._aggregate_invoices(self.processed_data)

            # Generate metadata
            self.metadata = self._generate_metadata()

            return True, {
                'message': 'File uploaded and processed successfully',
                'metadata': self.metadata,
                'validation_report': validation_report
            }

        except Exception as e:
            return False, {
                'error': f'Upload failed: {str(e)}'
            }

    def _read_excel(self, file_path: str) -> pd.DataFrame:
        """
        Read Excel file with appropriate engine

        Args:
            file_path: Path to Excel file

        Returns:
            Raw DataFrame
        """
        file_extension = Path(file_path).suffix.lower()

        if file_extension == '.xlsx':
            df = pd.read_excel(file_path, engine='openpyxl')
        elif file_extension == '.xls':
            df = pd.read_excel(file_path, engine='xlrd')
        else:
            raise ValueError(f"Unsupported file format: {file_extension}")

        # Map column names to standard format
        df = self._map_columns(df)

        return df

    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Map various column name formats to standard names

        Args:
            df: DataFrame with original column names

        Returns:
            DataFrame with standardized column names
        """
        # Column mapping dictionary
        column_map = {
            # Invoice number variations
            'Inv#': 'Inv#',
            'Invoice': 'Inv#',
            'Invoice Number': 'Inv#',
            'InvoiceNo': 'Inv#',

            # Date variations
            'Date': 'Date',
            'Transaction Date': 'Date',
            'Invoice Date': 'Date',

            # Customer variations
            'Customer': 'Customer',
            'Customer Name': 'Customer',
            'Client': 'Customer',

            # Sales Rep variations (use SalesMan as primary, ignore SlMan)
            'Sales Rep': 'Sales Rep',
            'SalesMan': 'Sales Rep',
            'Salesman': 'Sales Rep',
            'Representative': 'Sales Rep',

            # Production Line variations
            'Production Line': 'Production Line',
            'ProdLine#': 'Production Line',
            'Product Line': 'Production Line',
            'Line': 'Production Line',

            # Amount variations
            'Total Amount': 'Total Amount',
            'Amount': 'Total Amount',
            'Total': 'Total Amount',
            'Value': 'Total Amount',

            # Optional columns
            'Item': 'Item',
            'Product': 'Item',
            'Quantity': 'Quantity',
            'Qty': 'Quantity',
            'Unit Price': 'Unit Price',
            'Prc': 'Unit Price',
            'Price': 'Unit Price',
            'Group Item': 'Group Item',
            'GrpItem': 'Group Item',
            'Size': 'Size',
            'ItemSZ': 'Size'
        }

        # Rename columns that match the mapping
        df = df.rename(columns=column_map)

        # Drop duplicate columns if any (like SlMan when we already have SalesMan)
        df = df.loc[:, ~df.columns.duplicated(keep='first')]

        return df

    def _process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process and normalize raw data

        Args:
            df: Raw DataFrame

        Returns:
            Processed DataFrame with normalized columns
        """
        df = df.copy()

        # 0. Filter out rows with missing invoice numbers
        if 'Inv#' in df.columns:
            df = df[df['Inv#'].notna()].copy()

        # 1. Parse and extract date components
        df = parse_date_column(df, 'Date')

        # 2. Normalize numeric columns
        df = self._normalize_numeric_columns(df)

        # 3. Identify returns (negative amounts)
        df = identify_returns(df, 'Total Amount')

        # 4. Clean and normalize text columns
        df = self._normalize_text_columns(df)

        # 5. Add transaction type
        df['Transaction_Type'] = df['Is_Return'].map({
            True: 'Return',
            False: 'Sale'
        })

        # 6. Calculate absolute amount for aggregations
        df['Absolute_Amount'] = df['Total Amount'].abs()

        # 7. Sort by invoice and date
        df = df.sort_values(['Inv#', 'Date']).reset_index(drop=True)

        return df

    def _normalize_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize numeric columns to proper data types

        Args:
            df: DataFrame to normalize

        Returns:
            DataFrame with normalized numeric columns
        """
        df = df.copy()

        # Convert Total Amount to numeric
        df['Total Amount'] = pd.to_numeric(df['Total Amount'], errors='coerce')

        # Convert Quantity if present
        if 'Quantity' in df.columns:
            df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')
            df['Quantity'] = df['Quantity'].fillna(1)  # Default to 1 if missing

        # Convert Unit Price if present
        if 'Unit Price' in df.columns:
            df['Unit Price'] = pd.to_numeric(df['Unit Price'], errors='coerce')

        return df

    def _normalize_text_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and normalize text columns

        Args:
            df: DataFrame to normalize

        Returns:
            DataFrame with normalized text columns
        """
        df = df.copy()

        text_columns = ['Customer', 'Sales Rep', 'Production Line']

        # Add optional text columns if they exist
        optional_text_cols = ['Item', 'Group Item', 'Size']
        for col in optional_text_cols:
            if col in df.columns:
                text_columns.append(col)

        # Clean text columns
        for col in text_columns:
            if col in df.columns:
                # Strip whitespace
                df[col] = df[col].astype(str).str.strip()

                # Replace empty strings with 'Unknown'
                df[col] = df[col].replace(['', 'nan', 'None'], 'Unknown')

                # Title case for better consistency
                df[col] = df[col].str.title()

        return df

    def _aggregate_invoices(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate data at invoice level
        CRITICAL: This is where invoice-level metrics are calculated

        Args:
            df: Processed line-item DataFrame

        Returns:
            Invoice-level aggregated DataFrame
        """
        # Group by invoice number
        invoice_groups = df.groupby('Inv#')

        # Aggregate invoice-level metrics
        invoice_agg = invoice_groups.agg({
            'Date': 'first',  # Invoice date (first line item date)
            'Customer': 'first',  # Customer (should be same for all lines)
            'Sales Rep': 'first',  # Sales rep (should be same for all lines)
            'Production Line': lambda x: x.mode()[0] if len(x.mode()) > 0 else x.iloc[0],  # Most common
            'Total Amount': 'sum',  # Total invoice amount (SUM of all line items)
            'Absolute_Amount': 'sum',  # Sum of absolute amounts
            'Is_Return': 'any',  # True if any line item is a return
            'Transaction_Type': lambda x: 'Mixed' if len(x.unique()) > 1 else x.iloc[0]
        }).reset_index()

        # Add count of line items per invoice
        invoice_agg['Line_Item_Count'] = invoice_groups.size().values

        # Add invoice count (each row is one invoice)
        invoice_agg['Invoice_Count'] = 1

        # Classify invoice type based on total amount
        invoice_agg['Invoice_Type'] = np.where(
            invoice_agg['Total Amount'] < 0,
            'Full Return',
            np.where(
                invoice_agg['Total Amount'] == 0,
                'Canceled',
                'Sale'
            )
        )

        # Parse date components for invoice-level data
        invoice_agg = parse_date_column(invoice_agg, 'Date')

        return invoice_agg

    def _generate_metadata(self) -> Dict:
        """
        Generate metadata about the loaded dataset

        Returns:
            Dictionary with dataset statistics
        """
        if self.processed_data is None or self.invoice_data is None:
            return {}

        metadata = {
            'upload_timestamp': datetime.now().isoformat(),
            'line_items': {
                'total_count': len(self.processed_data),
                'sales_count': len(self.processed_data[~self.processed_data['Is_Return']]),
                'returns_count': len(self.processed_data[self.processed_data['Is_Return']])
            },
            'invoices': {
                'total_count': len(self.invoice_data),
                'sales_count': len(self.invoice_data[self.invoice_data['Invoice_Type'] == 'Sale']),
                'returns_count': len(self.invoice_data[self.invoice_data['Invoice_Type'] == 'Full Return']),
                'canceled_count': len(self.invoice_data[self.invoice_data['Invoice_Type'] == 'Canceled'])
            },
            'date_range': {
                'start_date': self.processed_data['Date'].min().strftime('%Y-%m-%d'),
                'end_date': self.processed_data['Date'].max().strftime('%Y-%m-%d'),
                'years': sorted(self.processed_data['Year'].unique().tolist()),
                'months_covered': len(self.processed_data.groupby(['Year', 'Month']))
            },
            'dimensions': {
                'customers': self.processed_data['Customer'].nunique(),
                'sales_reps': self.processed_data['Sales Rep'].nunique(),
                'production_lines': self.processed_data['Production Line'].nunique()
            },
            'amounts': {
                'total_sales': float(self.processed_data[~self.processed_data['Is_Return']]['Total Amount'].sum()),
                'total_returns': float(self.processed_data[self.processed_data['Is_Return']]['Total Amount'].sum()),
                'net_sales': float(self.processed_data['Total Amount'].sum())
            }
        }

        # Add optional dimension counts
        if 'Item' in self.processed_data.columns:
            metadata['dimensions']['items'] = self.processed_data['Item'].nunique()

        if 'Group Item' in self.processed_data.columns:
            metadata['dimensions']['item_groups'] = self.processed_data['Group Item'].nunique()

        return metadata

    def get_data(self) -> Optional[pd.DataFrame]:
        """
        Get processed line-item level data

        Returns:
            Processed DataFrame or None
        """
        return self.processed_data

    def get_invoice_data(self) -> Optional[pd.DataFrame]:
        """
        Get invoice-level aggregated data

        Returns:
            Invoice DataFrame or None
        """
        return self.invoice_data

    def get_metadata(self) -> Dict:
        """
        Get dataset metadata

        Returns:
            Metadata dictionary
        """
        return self.metadata

    def clear_data(self):
        """Clear all loaded data"""
        self.raw_data = None
        self.processed_data = None
        self.invoice_data = None
        self.metadata = {}
