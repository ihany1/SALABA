"""
Filtering Engine
High-performance server-side filtering with AND logic
Stateless design for scalability
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))
from utils.helpers import create_date_range_filter


class FilteringEngine:
    """
    Engine for applying filters to datasets
    Supports multi-dimensional AND filtering
    """

    @staticmethod
    def apply_filters(df: pd.DataFrame, filters: Dict[str, Any]) -> pd.DataFrame:
        """
        Apply filters to DataFrame using AND logic

        Args:
            df: DataFrame to filter
            filters: Dictionary of filters
                {
                    'year': [2023, 2024],
                    'quarter': [1, 2],
                    'month': [1, 2, 3],
                    'week': [1, 2, 3],
                    'day': [1, 15, 30],
                    'sales_rep': ['Rep A', 'Rep B'],
                    'customer': ['Customer X'],
                    'production_line': ['Line 1', 'Line 2'],
                    'group_item': ['Group A'],
                    'size': ['Large'],
                    'item': ['Item 1'],
                    'date_start': '2023-01-01',
                    'date_end': '2023-12-31',
                    'transaction_type': 'Sale' | 'Return' | 'All'
                }

        Returns:
            Filtered DataFrame
        """
        if not filters or len(filters) == 0:
            return df

        # Start with all rows
        mask = pd.Series([True] * len(df), index=df.index)

        # Apply each filter with AND logic
        for filter_key, filter_value in filters.items():
            if filter_value is None or filter_value == [] or filter_value == '':
                continue

            # Apply specific filter
            filter_mask = FilteringEngine._apply_single_filter(
                df, filter_key, filter_value
            )

            if filter_mask is not None:
                mask &= filter_mask

        return df[mask].reset_index(drop=True)

    @staticmethod
    def _apply_single_filter(df: pd.DataFrame,
                              filter_key: str,
                              filter_value: Any) -> Optional[pd.Series]:
        """
        Apply a single filter condition

        Args:
            df: DataFrame
            filter_key: Filter field name
            filter_value: Filter value(s)

        Returns:
            Boolean Series for filtering
        """
        # Time-based filters
        if filter_key == 'year':
            return FilteringEngine._filter_list(df, 'Year', filter_value)

        elif filter_key == 'quarter':
            return FilteringEngine._filter_list(df, 'Quarter', filter_value)

        elif filter_key == 'month':
            return FilteringEngine._filter_list(df, 'Month', filter_value)

        elif filter_key == 'week':
            if 'Week' in df.columns:
                return FilteringEngine._filter_list(df, 'Week', filter_value)

        elif filter_key == 'day':
            if 'Day' in df.columns:
                return FilteringEngine._filter_list(df, 'Day', filter_value)

        # Date range filters
        elif filter_key == 'date_start':
            if 'Date' in df.columns:
                return df['Date'] >= pd.to_datetime(filter_value)

        elif filter_key == 'date_end':
            if 'Date' in df.columns:
                return df['Date'] <= pd.to_datetime(filter_value)

        # Dimension filters
        elif filter_key == 'sales_rep':
            return FilteringEngine._filter_list(df, 'Sales Rep', filter_value)

        elif filter_key == 'customer':
            return FilteringEngine._filter_list(df, 'Customer', filter_value)

        elif filter_key == 'production_line':
            return FilteringEngine._filter_list(df, 'Production Line', filter_value)

        elif filter_key == 'group_item':
            if 'Group Item' in df.columns:
                return FilteringEngine._filter_list(df, 'Group Item', filter_value)

        elif filter_key == 'size':
            if 'Size' in df.columns:
                return FilteringEngine._filter_list(df, 'Size', filter_value)

        elif filter_key == 'item':
            if 'Item' in df.columns:
                return FilteringEngine._filter_list(df, 'Item', filter_value)

        # Material group filter (extracted from Item)
        elif filter_key == 'material_group':
            if 'Item' in df.columns:
                # Extract material group on-the-fly for filtering
                material_groups = df['Item'].apply(FilteringEngine._extract_material_group)
                if isinstance(filter_value, list):
                    return material_groups.isin(filter_value)
                else:
                    return material_groups == filter_value

        # Size filter (extracted from Item)
        elif filter_key == 'size':
            if 'Item' in df.columns:
                # Extract size on-the-fly for filtering
                sizes = df['Item'].apply(FilteringEngine._extract_size)
                if isinstance(filter_value, list):
                    return sizes.isin(filter_value)
                else:
                    return sizes == filter_value

        # Transaction type filter
        elif filter_key == 'transaction_type':
            if filter_value.lower() == 'sale':
                return ~df['Is_Return']
            elif filter_value.lower() == 'return':
                return df['Is_Return']
            # 'All' or any other value returns all rows

        return None

    @staticmethod
    def _extract_material_group(item_name: str) -> str:
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

    @staticmethod
    def _extract_size(item_name: str) -> str:
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

    @staticmethod
    def _filter_list(df: pd.DataFrame, column: str, values: Any) -> pd.Series:
        """
        Filter by list of values (OR within the list)

        Args:
            df: DataFrame
            column: Column name
            values: Single value or list of values

        Returns:
            Boolean Series
        """
        if column not in df.columns:
            return pd.Series([True] * len(df), index=df.index)

        # Convert single value to list
        if not isinstance(values, list):
            values = [values]

        return df[column].isin(values)

    @staticmethod
    def get_active_filter_count(filters: Dict[str, Any]) -> int:
        """
        Count how many filters are active

        Args:
            filters: Filter dictionary

        Returns:
            Number of active filters
        """
        count = 0
        for key, value in filters.items():
            if value is not None and value != [] and value != '':
                count += 1
        return count

    @staticmethod
    def get_filter_summary(filters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a summary of active filters

        Args:
            filters: Filter dictionary

        Returns:
            Summary dictionary
        """
        summary = {
            'active_count': FilteringEngine.get_active_filter_count(filters),
            'active_filters': {}
        }

        for key, value in filters.items():
            if value is not None and value != [] and value != '':
                summary['active_filters'][key] = value

        return summary

    @staticmethod
    def validate_filters(filters: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate filter structure and values

        Args:
            filters: Filter dictionary

        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []

        # Validate date formats
        if 'date_start' in filters and filters['date_start']:
            try:
                pd.to_datetime(filters['date_start'])
            except Exception:
                errors.append(f"Invalid date_start format: {filters['date_start']}")

        if 'date_end' in filters and filters['date_end']:
            try:
                pd.to_datetime(filters['date_end'])
            except Exception:
                errors.append(f"Invalid date_end format: {filters['date_end']}")

        # Validate numeric filters
        numeric_filters = ['year', 'quarter', 'month', 'week', 'day']
        for filter_key in numeric_filters:
            if filter_key in filters and filters[filter_key]:
                values = filters[filter_key] if isinstance(filters[filter_key], list) else [filters[filter_key]]
                for value in values:
                    if not isinstance(value, (int, float)):
                        errors.append(f"Invalid {filter_key} value: {value} (must be numeric)")

        is_valid = len(errors) == 0
        return is_valid, errors

    @staticmethod
    def build_filter_description(filters: Dict[str, Any]) -> str:
        """
        Build human-readable description of filters

        Args:
            filters: Filter dictionary

        Returns:
            Descriptive string
        """
        if not filters or FilteringEngine.get_active_filter_count(filters) == 0:
            return "No filters applied (showing all data)"

        parts = []

        # Time filters
        if 'year' in filters and filters['year']:
            years = filters['year'] if isinstance(filters['year'], list) else [filters['year']]
            parts.append(f"Year: {', '.join(map(str, years))}")

        if 'quarter' in filters and filters['quarter']:
            quarters = filters['quarter'] if isinstance(filters['quarter'], list) else [filters['quarter']]
            parts.append(f"Quarter: {', '.join(map(str, quarters))}")

        if 'month' in filters and filters['month']:
            months = filters['month'] if isinstance(filters['month'], list) else [filters['month']]
            parts.append(f"Month: {', '.join(map(str, months))}")

        # Date range
        if 'date_start' in filters and filters['date_start']:
            parts.append(f"From: {filters['date_start']}")

        if 'date_end' in filters and filters['date_end']:
            parts.append(f"To: {filters['date_end']}")

        # Dimension filters
        if 'sales_rep' in filters and filters['sales_rep']:
            reps = filters['sales_rep'] if isinstance(filters['sales_rep'], list) else [filters['sales_rep']]
            parts.append(f"Rep: {', '.join(reps)}")

        if 'customer' in filters and filters['customer']:
            customers = filters['customer'] if isinstance(filters['customer'], list) else [filters['customer']]
            parts.append(f"Customer: {', '.join(customers)}")

        if 'production_line' in filters and filters['production_line']:
            lines = filters['production_line'] if isinstance(filters['production_line'], list) else [filters['production_line']]
            parts.append(f"Line: {', '.join(lines)}")

        # Transaction type
        if 'transaction_type' in filters and filters['transaction_type']:
            parts.append(f"Type: {filters['transaction_type']}")

        return " | ".join(parts)
