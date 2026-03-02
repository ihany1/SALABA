"""
Data Manager Service
Singleton service to manage application data state
Holds current dataset in memory (PostgreSQL-ready for future migration)
"""

import pandas as pd
from typing import Optional, Dict
from pathlib import Path
from datetime import datetime


class DataManager:
    """
    Singleton class to manage application data state
    Thread-safe data storage for current session
    """

    _instance = None
    _initialized = False

    def __new__(cls):
        """Ensure only one instance exists (Singleton pattern)"""
        if cls._instance is None:
            cls._instance = super(DataManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize data manager (only once)"""
        if not DataManager._initialized:
            self._line_item_data: Optional[pd.DataFrame] = None
            self._invoice_data: Optional[pd.DataFrame] = None
            self._metadata: Dict = {}
            self._is_loaded: bool = False
            self._upload_time: Optional[datetime] = None
            self._source_filename: Optional[str] = None
            DataManager._initialized = True

    def set_data(self,
                 line_item_data: pd.DataFrame,
                 invoice_data: pd.DataFrame,
                 metadata: Dict):
        """
        Set the current dataset

        Args:
            line_item_data: Processed line-item level data
            invoice_data: Invoice-level aggregated data
            metadata: Dataset metadata
        """
        self._line_item_data = line_item_data.copy()
        self._invoice_data = invoice_data.copy()
        self._metadata = metadata.copy()
        self._is_loaded = True
        self._upload_time = datetime.now()

    def get_line_item_data(self) -> Optional[pd.DataFrame]:
        """
        Get line-item level data

        Returns:
            DataFrame with all line items or None
        """
        if self._line_item_data is not None:
            return self._line_item_data.copy()
        return None

    def get_invoice_data(self) -> Optional[pd.DataFrame]:
        """
        Get invoice-level data

        Returns:
            DataFrame with invoice aggregations or None
        """
        if self._invoice_data is not None:
            return self._invoice_data.copy()
        return None

    def get_metadata(self) -> Dict:
        """
        Get dataset metadata

        Returns:
            Metadata dictionary
        """
        return self._metadata.copy()

    def is_data_loaded(self) -> bool:
        """
        Check if data is currently loaded

        Returns:
            True if data is loaded
        """
        return self._is_loaded

    def set_source_filename(self, filename: str):
        """Set the source filename"""
        self._source_filename = filename

    def get_freshness_info(self) -> Dict:
        """Get data freshness information"""
        if not self._is_loaded:
            return {}

        now = datetime.now()
        elapsed = now - self._upload_time if self._upload_time else None

        # Calculate completeness
        completeness = 100.0
        if self._line_item_data is not None:
            total_cells = self._line_item_data.size
            null_cells = int(self._line_item_data.isnull().sum().sum())
            completeness = round((1 - null_cells / max(total_cells, 1)) * 100, 1)

        return {
            'upload_time': self._upload_time.isoformat() if self._upload_time else None,
            'elapsed_seconds': int(elapsed.total_seconds()) if elapsed else None,
            'completeness': completeness,
            'source': self._source_filename or 'Unknown',
            'row_count': len(self._line_item_data) if self._line_item_data is not None else 0
        }

    def clear_data(self):
        """Clear all data from memory"""
        self._line_item_data = None
        self._invoice_data = None
        self._metadata = {}
        self._is_loaded = False
        self._upload_time = None
        self._source_filename = None

    def get_filter_options(self) -> Dict:
        """
        Get all unique values for filter dropdowns
        Extracts material groups and sizes from Item column

        Returns:
            Dictionary with filter options for each dimension
        """
        if not self._is_loaded or self._line_item_data is None:
            return {}

        df = self._line_item_data.copy()

        # Extract material groups and sizes from Item column
        if 'Item' in df.columns:
            df['Material_Group'] = df['Item'].apply(self._extract_material_group)
            df['Size_Extracted'] = df['Item'].apply(self._extract_size)

        options = {
            'years': sorted(df['Year'].unique().tolist()) if 'Year' in df.columns else [],
            'quarters': sorted(df['Quarter'].unique().tolist()) if 'Quarter' in df.columns else [],
            'months': sorted(df['Month'].unique().tolist()) if 'Month' in df.columns else [],
            'sales_reps': sorted(df['Sales Rep'].unique().tolist()) if 'Sales Rep' in df.columns else [],
            'customers': sorted(df['Customer'].unique().tolist()) if 'Customer' in df.columns else [],
            'production_lines': sorted(df['Production Line'].unique().tolist()) if 'Production Line' in df.columns else []
        }

        # Add material groups (extracted)
        if 'Material_Group' in df.columns:
            groups = df['Material_Group'].unique().tolist()
            groups = [g for g in groups if g and g != 'Other' and g != 'N/A']
            options['material_groups'] = sorted(groups)

        # Add sizes (extracted)
        if 'Size_Extracted' in df.columns:
            sizes = df['Size_Extracted'].unique().tolist()
            sizes = [s for s in sizes if s and s != 'N/A']
            options['sizes'] = sorted(sizes)

        # Add optional dimensions
        if 'Group Item' in df.columns:
            options['group_items'] = sorted(df['Group Item'].unique().tolist())

        if 'Item' in df.columns:
            # Limit items to top 100 to avoid overwhelming dropdown
            top_items = df['Item'].value_counts().head(100).index.tolist()
            options['items'] = sorted(top_items)

        return options

    def _extract_material_group(self, item_name: str) -> str:
        """Extract material group from item name"""
        if not isinstance(item_name, str):
            return 'Other'

        item_lower = item_name.lower().strip()

        if 'interlock' in item_lower:
            return 'Interlock'
        elif 'curbstone' in item_lower or 'curb' in item_lower:
            return 'Curbstone'
        elif 'cement tiles' in item_lower or ('cement' in item_lower and 'tiles' in item_lower):
            return 'Cement Tiles'
        elif 'cbl tiles' in item_lower or 'cbl' in item_lower:
            return 'CBL Tiles'
        elif 'paver' in item_lower:
            return 'Pavers'
        elif 'block' in item_lower:
            return 'Blocks'
        elif 'charge' in item_lower:
            return 'Charges'
        else:
            return 'Other'

    def _extract_size(self, item_name: str) -> str:
        """Extract size from item name using regex"""
        import re

        if not isinstance(item_name, str):
            return 'N/A'

        # Try to find patterns like "6Cm", "8Cm", "10Cm"
        cm_match = re.search(r'(\d+)Cm', item_name, re.IGNORECASE)
        if cm_match:
            return f"{cm_match.group(1)}cm"

        # Try to find patterns like "50*30*15"
        dim_match = re.search(r'(\d+\*\d+\*\d+)', item_name)
        if dim_match:
            return dim_match.group(1)

        return 'N/A'

    def get_date_range(self) -> Dict:
        """
        Get the date range of loaded data

        Returns:
            Dictionary with min and max dates
        """
        if not self._is_loaded or self._line_item_data is None:
            return {}

        return {
            'min_date': self._line_item_data['Date'].min().strftime('%Y-%m-%d'),
            'max_date': self._line_item_data['Date'].max().strftime('%Y-%m-%d')
        }

    def get_summary_stats(self) -> Dict:
        """
        Get quick summary statistics

        Returns:
            Dictionary with summary stats
        """
        if not self._is_loaded:
            return {}

        line_df = self._line_item_data
        inv_df = self._invoice_data

        return {
            'total_line_items': len(line_df),
            'total_invoices': len(inv_df),
            'unique_customers': line_df['Customer'].nunique(),
            'unique_reps': line_df['Sales Rep'].nunique(),
            'date_range': self.get_date_range(),
            'total_sales_amount': float(line_df[~line_df['Is_Return']]['Total Amount'].sum()),
            'total_returns_amount': float(line_df[line_df['Is_Return']]['Total Amount'].sum()),
            'net_sales': float(line_df['Total Amount'].sum())
        }


# Global instance getter
def get_data_manager() -> DataManager:
    """
    Get the singleton DataManager instance

    Returns:
        DataManager instance
    """
    return DataManager()
