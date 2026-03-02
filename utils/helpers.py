"""
Helper utility functions
General-purpose helpers for the application
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Any, Dict, List


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Safely divide two numbers, returning default if denominator is zero

    Args:
        numerator: Number to divide
        denominator: Number to divide by
        default: Value to return if division by zero

    Returns:
        Result of division or default value
    """
    if denominator == 0 or pd.isna(denominator):
        return default
    return numerator / denominator


def calculate_percentage(part: float, whole: float, decimals: int = 2) -> float:
    """
    Calculate percentage with safe division

    Args:
        part: Part value
        whole: Whole value
        decimals: Number of decimal places

    Returns:
        Percentage value
    """
    result = safe_divide(part, whole, 0.0) * 100
    return round(result, decimals)


def format_currency(amount: float, currency_symbol: str = '$') -> str:
    """
    Format number as currency string

    Args:
        amount: Amount to format
        currency_symbol: Currency symbol to use

    Returns:
        Formatted currency string
    """
    if pd.isna(amount):
        return f"{currency_symbol}0.00"
    return f"{currency_symbol}{amount:,.2f}"


def format_number(number: float, decimals: int = 2) -> str:
    """
    Format number with thousand separators

    Args:
        number: Number to format
        decimals: Number of decimal places

    Returns:
        Formatted number string
    """
    if pd.isna(number):
        return "0"
    return f"{number:,.{decimals}f}"


def parse_date_column(df: pd.DataFrame, date_column: str = 'Date') -> pd.DataFrame:
    """
    Parse and extract date components

    Args:
        df: DataFrame with date column
        date_column: Name of date column

    Returns:
        DataFrame with additional date component columns
    """
    df = df.copy()

    # Convert to datetime
    df[date_column] = pd.to_datetime(df[date_column])

    # Extract components
    df['Year'] = df[date_column].dt.year
    df['Quarter'] = df[date_column].dt.quarter
    df['Month'] = df[date_column].dt.month
    df['Month_Name'] = df[date_column].dt.strftime('%B')
    df['Week'] = df[date_column].dt.isocalendar().week
    df['Day'] = df[date_column].dt.day
    df['Day_of_Week'] = df[date_column].dt.day_name()
    df['Day_of_Year'] = df[date_column].dt.dayofyear

    return df


def identify_returns(df: pd.DataFrame, amount_column: str = 'Total Amount') -> pd.DataFrame:
    """
    Identify return transactions (negative amounts)

    Args:
        df: DataFrame with amount column
        amount_column: Name of amount column

    Returns:
        DataFrame with 'Is_Return' boolean column
    """
    df = df.copy()
    df['Is_Return'] = df[amount_column] < 0
    return df


def calculate_change(current: float, previous: float) -> Dict[str, Any]:
    """
    Calculate change between two values

    Args:
        current: Current period value
        previous: Previous period value

    Returns:
        Dictionary with absolute and percentage change
    """
    absolute_change = current - previous
    percentage_change = safe_divide(absolute_change, abs(previous), 0.0) * 100

    return {
        'current': current,
        'previous': previous,
        'absolute_change': absolute_change,
        'percentage_change': round(percentage_change, 2),
        'direction': 'up' if absolute_change > 0 else 'down' if absolute_change < 0 else 'unchanged'
    }


def get_top_n(df: pd.DataFrame, column: str, n: int = 10, ascending: bool = False) -> pd.DataFrame:
    """
    Get top N rows by a column value

    Args:
        df: DataFrame to filter
        column: Column to sort by
        n: Number of top items to return
        ascending: Sort in ascending order

    Returns:
        Top N rows
    """
    return df.nlargest(n, column) if not ascending else df.nsmallest(n, column)


def aggregate_others(df: pd.DataFrame, value_column: str, top_n: int = 10) -> pd.DataFrame:
    """
    Keep top N rows and aggregate the rest as 'Others'

    Args:
        df: DataFrame with data
        value_column: Column to use for ranking
        top_n: Number of top items to keep

    Returns:
        DataFrame with top N + 'Others' row
    """
    if len(df) <= top_n:
        return df

    df_sorted = df.sort_values(value_column, ascending=False)
    top = df_sorted.head(top_n).copy()
    others = df_sorted.tail(len(df_sorted) - top_n)

    # Create 'Others' row by summing numeric columns
    others_row = {}
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            others_row[col] = others[col].sum()
        elif col == df.columns[0]:  # Assume first column is the category
            others_row[col] = 'Others'
        else:
            others_row[col] = ''

    others_df = pd.DataFrame([others_row])
    result = pd.concat([top, others_df], ignore_index=True)

    return result


def create_date_range_filter(df: pd.DataFrame,
                              start_date: str = None,
                              end_date: str = None,
                              date_column: str = 'Date') -> pd.Series:
    """
    Create boolean mask for date range filtering

    Args:
        df: DataFrame with date column
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        date_column: Name of date column

    Returns:
        Boolean Series for filtering
    """
    mask = pd.Series([True] * len(df), index=df.index)

    if start_date:
        mask &= df[date_column] >= pd.to_datetime(start_date)

    if end_date:
        mask &= df[date_column] <= pd.to_datetime(end_date)

    return mask


def safe_int(value: Any, default: int = 0) -> int:
    """
    Safely convert a value to integer, handling NaN, None, infinity

    Args:
        value: Value to convert (can be float, int, NaN, None, etc.)
        default: Value to return if conversion fails

    Returns:
        Integer value or default

    Examples:
        safe_int(10.5) → 10
        safe_int(NaN) → 0
        safe_int(None) → 0
        safe_int(np.inf) → 0
        safe_int("abc") → 0
    """
    # Handle None
    if value is None:
        return default

    # Handle pandas/numpy NA values
    try:
        if pd.isna(value):
            return default
    except (ValueError, TypeError):
        pass

    # Handle numpy infinity
    try:
        if np.isinf(value):
            return default
    except (ValueError, TypeError):
        pass

    # Try to convert
    try:
        return int(value)
    except (ValueError, TypeError, OverflowError):
        return default


def safe_round(value: Any, decimals: int = 2, default: float = 0.0) -> float:
    """
    Safely round a value, handling NaN, None, infinity

    Args:
        value: Value to round
        decimals: Number of decimal places
        default: Value to return if rounding fails

    Returns:
        Rounded value or default
    """
    # Handle None
    if value is None:
        return default

    # Handle pandas/numpy NA values
    try:
        if pd.isna(value):
            return default
    except (ValueError, TypeError):
        pass

    # Handle infinity
    try:
        if np.isinf(value):
            return default
    except (ValueError, TypeError):
        pass

    # Try to round
    try:
        return round(value, decimals)
    except (ValueError, TypeError):
        return default


def calculate_growth_rate(values: List[float]) -> List[float]:
    """
    Calculate period-over-period growth rates

    Args:
        values: List of values in time order

    Returns:
        List of growth rates (first value is 0)
    """
    if len(values) < 2:
        return [0.0] * len(values)

    growth_rates = [0.0]  # First period has no growth

    for i in range(1, len(values)):
        rate = safe_divide(values[i] - values[i-1], abs(values[i-1]), 0.0) * 100
        growth_rates.append(round(rate, 2))

    return growth_rates


def sanitize_for_json(obj: Any) -> Any:
    """
    Convert numpy/pandas types to native Python types for JSON serialization

    Args:
        obj: Object to sanitize (dict, list, numpy type, pandas type, etc.)

    Returns:
        JSON-serializable version of the object
    """
    # Handle None FIRST
    if obj is None:
        return None

    # Handle pandas Series/Index BEFORE pd.isna() check
    # CRITICAL: Must check type BEFORE using pd.isna() to avoid ambiguity error
    if isinstance(obj, (pd.Series, pd.Index)):
        return sanitize_for_json(obj.tolist())

    # Handle dictionaries recursively
    if isinstance(obj, dict):
        return {key: sanitize_for_json(value) for key, value in obj.items()}

    # Handle lists/tuples recursively
    if isinstance(obj, (list, tuple)):
        return [sanitize_for_json(item) for item in obj]

    # NOW safe to check pd.isna() on scalar values only
    # FIXED: Only check pd.isna() AFTER confirming it's not a Series/array
    try:
        if pd.isna(obj):
            return None
    except (ValueError, TypeError):
        # If pd.isna() fails (e.g., on complex objects), skip this check
        pass

    # Handle numpy integer types (use safe_int to handle NaN edge cases)
    if isinstance(obj, (np.integer, np.int64, np.int32, np.int16, np.int8)):
        return safe_int(obj, default=0)

    # Handle numpy float types
    if isinstance(obj, (np.floating, np.float64, np.float32)):
        # Check for NaN/Inf using numpy functions (safe for scalars)
        try:
            if np.isnan(obj) or np.isinf(obj):
                return None
        except (ValueError, TypeError):
            pass
        return float(obj)

    # Handle numpy bool
    if isinstance(obj, np.bool_):
        return bool(obj)

    # Handle datetime/timedelta
    if isinstance(obj, (datetime, pd.Timestamp)):
        return obj.isoformat()

    if isinstance(obj, (timedelta, pd.Timedelta)):
        try:
            return int(obj.total_seconds())
        except (ValueError, TypeError, AttributeError):
            return 0

    # Already JSON-serializable types
    if isinstance(obj, (str, int, float, bool)):
        return obj

    # Fallback for unknown types - convert to string
    return str(obj)
