"""
Core Aggregation Engine
Handles all invoice-level aware aggregations and metrics
CRITICAL: All calculations respect invoice boundaries
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))
from utils.helpers import safe_divide, calculate_percentage


class AggregationEngine:
    """
    Core engine for aggregating sales data
    All methods are invoice-level aware
    """

    @staticmethod
    def aggregate_by_invoice(df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate line items to invoice level

        Args:
            df: Line-item DataFrame

        Returns:
            Invoice-level DataFrame
        """
        invoice_groups = df.groupby('Inv#')

        invoice_agg = invoice_groups.agg({
            'Date': 'first',
            'Customer': 'first',
            'Sales Rep': 'first',
            'Production Line': lambda x: x.mode()[0] if len(x.mode()) > 0 else x.iloc[0],
            'Total Amount': 'sum',
            'Is_Return': 'any',
            'Year': 'first',
            'Quarter': 'first',
            'Month': 'first',
            'Week': 'first'
        }).reset_index()

        invoice_agg['Invoice_Count'] = 1  # Each row is one invoice
        invoice_agg['Line_Item_Count'] = invoice_groups.size().values

        return invoice_agg

    @staticmethod
    def aggregate_by_time(df: pd.DataFrame,
                          time_dimension: str = 'Month',
                          is_invoice_level: bool = True) -> pd.DataFrame:
        """
        Aggregate by time period

        Args:
            df: DataFrame (invoice or line-item level)
            time_dimension: 'Year', 'Quarter', 'Month', 'Week', 'Day'
            is_invoice_level: True if df is already invoice-level

        Returns:
            Time-aggregated DataFrame
        """
        # If not invoice level, aggregate first
        if not is_invoice_level:
            df = AggregationEngine.aggregate_by_invoice(df)

        # Group by time dimension
        time_agg = df.groupby(time_dimension).agg({
            'Total Amount': 'sum',
            'Invoice_Count': 'sum' if 'Invoice_Count' in df.columns else 'count',
            'Line_Item_Count': 'sum' if 'Line_Item_Count' in df.columns else 'count'
        }).reset_index()

        # Calculate derived metrics
        time_agg['Avg_Invoice_Value'] = time_agg.apply(
            lambda row: safe_divide(row['Total Amount'], row['Invoice_Count']),
            axis=1
        )

        return time_agg

    @staticmethod
    def aggregate_by_customer(df: pd.DataFrame,
                               is_invoice_level: bool = False) -> pd.DataFrame:
        """
        Aggregate by customer (invoice-level aware)

        Args:
            df: DataFrame
            is_invoice_level: True if df is already invoice-level

        Returns:
            Customer-aggregated DataFrame
        """
        # If not invoice level, aggregate first
        if not is_invoice_level:
            df = AggregationEngine.aggregate_by_invoice(df)

        customer_agg = df.groupby('Customer').agg({
            'Total Amount': 'sum',
            'Invoice_Count': 'sum' if 'Invoice_Count' in df.columns else 'count',
            'Is_Return': 'sum'  # Count of invoices with returns
        }).reset_index()

        customer_agg.columns = ['Customer', 'Total_Sales', 'Invoice_Count', 'Return_Invoice_Count']

        # Calculate metrics
        customer_agg['Avg_Invoice_Value'] = customer_agg.apply(
            lambda row: safe_divide(row['Total_Sales'], row['Invoice_Count']),
            axis=1
        )

        customer_agg['Return_Rate'] = customer_agg.apply(
            lambda row: calculate_percentage(row['Return_Invoice_Count'], row['Invoice_Count']),
            axis=1
        )

        # Sort by total sales
        customer_agg = customer_agg.sort_values('Total_Sales', ascending=False).reset_index(drop=True)

        return customer_agg

    @staticmethod
    def aggregate_by_rep(df: pd.DataFrame,
                         is_invoice_level: bool = False) -> pd.DataFrame:
        """
        Aggregate by sales representative (invoice-level aware)

        Args:
            df: DataFrame
            is_invoice_level: True if df is already invoice-level

        Returns:
            Rep-aggregated DataFrame
        """
        # If not invoice level, aggregate first
        if not is_invoice_level:
            df = AggregationEngine.aggregate_by_invoice(df)

        rep_agg = df.groupby('Sales Rep').agg({
            'Total Amount': 'sum',
            'Invoice_Count': 'sum' if 'Invoice_Count' in df.columns else 'count',
            'Is_Return': 'sum',
            'Customer': 'nunique'  # Unique customers per rep
        }).reset_index()

        rep_agg.columns = ['Sales_Rep', 'Total_Sales', 'Invoice_Count',
                           'Return_Invoice_Count', 'Unique_Customers']

        # Calculate metrics
        rep_agg['Avg_Invoice_Value'] = rep_agg.apply(
            lambda row: safe_divide(row['Total_Sales'], row['Invoice_Count']),
            axis=1
        )

        rep_agg['Return_Rate'] = rep_agg.apply(
            lambda row: calculate_percentage(row['Return_Invoice_Count'], row['Invoice_Count']),
            axis=1
        )

        rep_agg['Avg_Sales_Per_Customer'] = rep_agg.apply(
            lambda row: safe_divide(row['Total_Sales'], row['Unique_Customers']),
            axis=1
        )

        # Sort by total sales
        rep_agg = rep_agg.sort_values('Total_Sales', ascending=False).reset_index(drop=True)

        return rep_agg

    @staticmethod
    def aggregate_by_product_line(df: pd.DataFrame,
                                   is_invoice_level: bool = False) -> pd.DataFrame:
        """
        Aggregate by production line (invoice-level aware)

        Args:
            df: DataFrame
            is_invoice_level: True if df is already invoice-level

        Returns:
            Product line-aggregated DataFrame
        """
        # For product lines, we use line-item level data
        # because one invoice can have multiple product lines

        if is_invoice_level:
            # Cannot aggregate product lines from invoice level
            # Need line-item data
            raise ValueError("Product line aggregation requires line-item level data")

        line_agg = df.groupby('Production Line').agg({
            'Total Amount': 'sum',
            'Inv#': 'nunique',  # Unique invoices
            'Is_Return': 'sum'  # Count of return line items
        }).reset_index()

        line_agg.columns = ['Production_Line', 'Total_Sales', 'Invoice_Count', 'Return_Count']

        # Calculate metrics
        line_agg['Avg_Value_Per_Invoice'] = line_agg.apply(
            lambda row: safe_divide(row['Total_Sales'], row['Invoice_Count']),
            axis=1
        )

        # Sort by total sales
        line_agg = line_agg.sort_values('Total_Sales', ascending=False).reset_index(drop=True)

        return line_agg

    @staticmethod
    def calculate_sales_vs_returns(df: pd.DataFrame,
                                    is_invoice_level: bool = False) -> Dict:
        """
        Calculate sales vs returns breakdown

        Args:
            df: DataFrame
            is_invoice_level: True if df is already invoice-level

        Returns:
            Dictionary with sales and returns metrics
        """
        if not is_invoice_level:
            # For line-item level, separate sales and returns
            sales_df = df[~df['Is_Return']]
            returns_df = df[df['Is_Return']]

            gross_sales = float(sales_df['Total Amount'].sum())
            total_returns = float(returns_df['Total Amount'].sum())
            net_sales = gross_sales + total_returns  # Returns are negative

            sales_invoice_count = sales_df['Inv#'].nunique()
            return_invoice_count = returns_df['Inv#'].nunique()

        else:
            # For invoice level
            sales_df = df[~df['Is_Return']]
            returns_df = df[df['Is_Return']]

            gross_sales = float(sales_df['Total Amount'].sum())
            total_returns = float(returns_df['Total Amount'].sum())
            net_sales = gross_sales + total_returns

            sales_invoice_count = len(sales_df)
            return_invoice_count = len(returns_df)

        total_invoice_count = sales_invoice_count + return_invoice_count

        return {
            'gross_sales': gross_sales,
            'total_returns': total_returns,
            'net_sales': net_sales,
            'sales_invoice_count': sales_invoice_count,
            'return_invoice_count': return_invoice_count,
            'total_invoice_count': total_invoice_count,
            'return_rate': calculate_percentage(return_invoice_count, total_invoice_count),
            'avg_sales_invoice_value': safe_divide(gross_sales, sales_invoice_count),
            'avg_return_invoice_value': safe_divide(abs(total_returns), return_invoice_count)
        }

    @staticmethod
    def calculate_contribution_percentages(df: pd.DataFrame,
                                            group_column: str,
                                            value_column: str = 'Total Amount') -> pd.DataFrame:
        """
        Calculate contribution percentage for each group

        Args:
            df: DataFrame
            group_column: Column to group by
            value_column: Column to sum

        Returns:
            DataFrame with contribution percentages
        """
        grouped = df.groupby(group_column)[value_column].sum().reset_index()
        total = grouped[value_column].sum()

        grouped['Contribution_Pct'] = grouped[value_column].apply(
            lambda x: calculate_percentage(x, total)
        )

        grouped = grouped.sort_values(value_column, ascending=False).reset_index(drop=True)

        return grouped

    @staticmethod
    def calculate_period_comparison(df: pd.DataFrame,
                                     period_column: str = 'Month',
                                     is_invoice_level: bool = False) -> pd.DataFrame:
        """
        Calculate period-over-period changes

        Args:
            df: DataFrame
            period_column: Time period column
            is_invoice_level: True if df is already invoice-level

        Returns:
            DataFrame with period comparisons
        """
        # Aggregate by period
        period_agg = AggregationEngine.aggregate_by_time(df, period_column, is_invoice_level)

        # Sort by period
        period_agg = period_agg.sort_values(period_column).reset_index(drop=True)

        # Calculate changes
        period_agg['Total_Amount_Change'] = period_agg['Total Amount'].diff()
        period_agg['Total_Amount_Pct_Change'] = period_agg['Total Amount'].pct_change() * 100

        period_agg['Invoice_Count_Change'] = period_agg['Invoice_Count'].diff()
        period_agg['Invoice_Count_Pct_Change'] = period_agg['Invoice_Count'].pct_change() * 100

        return period_agg

    @staticmethod
    def get_top_n(df: pd.DataFrame,
                  n: int = 10,
                  sort_column: str = 'Total_Sales',
                  ascending: bool = False) -> pd.DataFrame:
        """
        Get top N records

        Args:
            df: DataFrame
            n: Number of records
            sort_column: Column to sort by
            ascending: Sort direction

        Returns:
            Top N DataFrame
        """
        return df.nlargest(n, sort_column) if not ascending else df.nsmallest(n, sort_column)

    @staticmethod
    def aggregate_others(df: pd.DataFrame,
                         n: int = 10,
                         value_column: str = 'Total_Sales',
                         label_column: str = None) -> pd.DataFrame:
        """
        Keep top N and aggregate rest as 'Others'

        Args:
            df: DataFrame
            n: Number of top items to keep
            value_column: Column to sum for 'Others'
            label_column: Column containing labels (first column if None)

        Returns:
            DataFrame with top N + Others row
        """
        if len(df) <= n:
            return df

        if label_column is None:
            label_column = df.columns[0]

        df_sorted = df.sort_values(value_column, ascending=False).reset_index(drop=True)
        top = df_sorted.head(n).copy()
        others = df_sorted.tail(len(df_sorted) - n)

        # Create Others row
        others_row = {}
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                others_row[col] = others[col].sum()
            elif col == label_column:
                others_row[col] = 'Others'
            else:
                others_row[col] = ''

        others_df = pd.DataFrame([others_row])
        result = pd.concat([top, others_df], ignore_index=True)

        return result
