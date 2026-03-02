"""
KPI (Key Performance Indicator) Engine
Calculates all business metrics with invoice-level awareness
Executive-level analytics calculations
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))
from utils.helpers import safe_divide, calculate_percentage, calculate_change, safe_int, safe_round
from services.aggregation_engine import AggregationEngine


class KPIEngine:
    """
    Engine for calculating Key Performance Indicators
    All calculations are invoice-level aware
    """

    @staticmethod
    def calculate_overview_kpis(line_data: pd.DataFrame,
                                 invoice_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate main overview KPIs

        Args:
            line_data: Line-item level data
            invoice_data: Invoice-level data

        Returns:
            Dictionary with overview KPIs
        """
        # Sales vs Returns breakdown
        sales_returns = AggregationEngine.calculate_sales_vs_returns(
            invoice_data, is_invoice_level=True
        )

        # Customer and Rep counts
        unique_customers = line_data['Customer'].nunique()
        unique_reps = line_data['Sales Rep'].nunique()
        unique_lines = line_data['Production Line'].nunique()

        # Time range (SAFE: Handle NaT/empty data)
        max_date = line_data['Date'].max()
        min_date = line_data['Date'].min()

        if pd.isna(max_date) or pd.isna(min_date):
            date_range_days = 0
        else:
            try:
                date_range_days = int((max_date - min_date).days)
            except (ValueError, TypeError):
                date_range_days = 0

        # Average line items per invoice
        avg_line_items = safe_divide(len(line_data), len(invoice_data))

        kpis = {
            # Core metrics
            'total_invoices': len(invoice_data),
            'total_line_items': len(line_data),
            'avg_line_items_per_invoice': safe_round(avg_line_items, 2),

            # Sales metrics (from sales_returns)
            'gross_sales': sales_returns['gross_sales'],
            'total_returns': sales_returns['total_returns'],
            'net_sales': sales_returns['net_sales'],

            # Invoice counts
            'sales_invoice_count': sales_returns['sales_invoice_count'],
            'return_invoice_count': sales_returns['return_invoice_count'],

            # Rates and averages
            'return_rate': sales_returns['return_rate'],
            'avg_invoice_value': sales_returns['avg_sales_invoice_value'],
            'avg_return_value': sales_returns['avg_return_invoice_value'],

            # Dimensions
            'unique_customers': unique_customers,
            'unique_sales_reps': unique_reps,
            'unique_product_lines': unique_lines,

            # Time metrics
            'date_range_days': date_range_days,
            'avg_invoices_per_day': safe_round(safe_divide(len(invoice_data), max(date_range_days, 1)), 2),

            # Per-rep metrics
            'avg_sales_per_rep': safe_round(safe_divide(sales_returns['gross_sales'], unique_reps), 0),
            'avg_invoices_per_rep': safe_round(safe_divide(len(invoice_data), unique_reps), 2),

            # Per-customer metrics
            'avg_sales_per_customer': safe_round(safe_divide(sales_returns['gross_sales'], unique_customers), 0),
            'avg_invoices_per_customer': safe_round(safe_divide(len(invoice_data), unique_customers), 2)
        }

        return kpis

    @staticmethod
    def calculate_sales_kpis(line_data: pd.DataFrame,
                              invoice_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate sales-specific KPIs

        Args:
            line_data: Line-item level data
            invoice_data: Invoice-level data

        Returns:
            Dictionary with sales KPIs
        """
        sales_returns = AggregationEngine.calculate_sales_vs_returns(
            invoice_data, is_invoice_level=True
        )

        # Filter only sales (non-returns)
        sales_invoices = invoice_data[~invoice_data['Is_Return']]
        returns_invoices = invoice_data[invoice_data['Is_Return']]

        # Sales distribution
        if len(sales_invoices) > 0:
            sales_stats = sales_invoices['Total Amount'].describe()

            kpis = {
                # Core metrics
                'gross_sales': sales_returns['gross_sales'],
                'total_returns': sales_returns['total_returns'],
                'net_sales': sales_returns['net_sales'],
                'sales_invoice_count': len(sales_invoices),
                'return_invoice_count': len(returns_invoices),
                'return_rate': sales_returns['return_rate'],

                # Distribution stats (exclude $0 invoices from min)
                'avg_invoice_value': safe_round(sales_stats.get('mean', 0), 2),
                'median_invoice_value': safe_round(sales_stats.get('50%', 0), 2),
                'min_invoice_value': safe_round(
                    sales_invoices.loc[sales_invoices['Total Amount'] > 0, 'Total Amount'].min()
                    if (sales_invoices['Total Amount'] > 0).any() else 0, 2
                ),
                'max_invoice_value': safe_round(sales_stats.get('max', 0), 2),
                'std_invoice_value': safe_round(sales_stats.get('std', 0), 2),

                # Percentiles
                'percentile_25': safe_round(sales_invoices['Total Amount'].quantile(0.25), 2),
                'percentile_75': safe_round(sales_invoices['Total Amount'].quantile(0.75), 2),
                'percentile_90': safe_round(sales_invoices['Total Amount'].quantile(0.90), 2),
                'percentile_95': safe_round(sales_invoices['Total Amount'].quantile(0.95), 2),
            }

            # Time-based trends
            monthly_sales = AggregationEngine.aggregate_by_time(
                sales_invoices, 'Month', is_invoice_level=True
            )

            kpis['monthly_trend'] = {
                'periods': monthly_sales['Month'].tolist() if len(monthly_sales) > 0 else [],
                'sales': monthly_sales['Total Amount'].tolist() if len(monthly_sales) > 0 else [],
                'invoice_counts': monthly_sales['Invoice_Count'].tolist() if len(monthly_sales) > 0 else [],
                'avg_values': monthly_sales['Avg_Invoice_Value'].tolist() if len(monthly_sales) > 0 else []
            }

            # Sales by production line
            line_sales = line_data.groupby('Production Line').agg({
                'Total Amount': 'sum'
            }).reset_index()
            line_sales.columns = ['Production_Line', 'Total_Sales']
            line_sales = line_sales.sort_values('Total_Sales', ascending=False)

            kpis['sales_by_line'] = {
                'labels': line_sales['Production_Line'].tolist() if len(line_sales) > 0 else [],
                'values': line_sales['Total_Sales'].tolist() if len(line_sales) > 0 else []
            }

            # Sales by quarter
            quarterly_sales = AggregationEngine.aggregate_by_time(
                sales_invoices, 'Quarter', is_invoice_level=True
            )

            kpis['quarterly_trend'] = {
                'periods': quarterly_sales['Quarter'].tolist() if len(quarterly_sales) > 0 else [],
                'sales': quarterly_sales['Total Amount'].tolist() if len(quarterly_sales) > 0 else []
            }

        else:
            kpis = {
                'gross_sales': 0,
                'total_returns': 0,
                'net_sales': 0,
                'sales_invoice_count': 0,
                'return_invoice_count': 0,
                'return_rate': 0,
                'avg_invoice_value': 0,
                'median_invoice_value': 0,
                'min_invoice_value': 0,
                'max_invoice_value': 0,
                'std_invoice_value': 0,
                'percentile_25': 0,
                'percentile_75': 0,
                'percentile_90': 0,
                'percentile_95': 0,
                'monthly_trend': {'periods': [], 'sales': [], 'invoice_counts': [], 'avg_values': []},
                'sales_by_line': {'labels': [], 'values': []},
                'quarterly_trend': {'periods': [], 'sales': []}
            }

        return kpis

    @staticmethod
    def calculate_rep_kpis(line_data: pd.DataFrame,
                            invoice_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate representative performance KPIs

        Args:
            line_data: Line-item level data
            invoice_data: Invoice-level data

        Returns:
            Dictionary with rep KPIs and rankings
        """
        # Aggregate by rep
        rep_agg = AggregationEngine.aggregate_by_rep(invoice_data, is_invoice_level=True)

        # Calculate rankings
        rep_agg['Sales_Rank'] = rep_agg['Total_Sales'].rank(ascending=False, method='dense')
        rep_agg['Invoice_Count_Rank'] = rep_agg['Invoice_Count'].rank(ascending=False, method='dense')

        # Calculate efficiency score (composite metric)
        # Score based on: sales amount, invoice count, low return rate, customer diversity
        max_sales = rep_agg['Total_Sales'].max()
        max_invoices = rep_agg['Invoice_Count'].max()
        max_customers = rep_agg['Unique_Customers'].max()

        rep_agg['Efficiency_Score'] = (
            (rep_agg['Total_Sales'] / max_sales * 40) +  # 40% weight on sales
            (rep_agg['Invoice_Count'] / max_invoices * 30) +  # 30% weight on invoice count
            (rep_agg['Unique_Customers'] / max_customers * 20) +  # 20% weight on customer diversity
            ((100 - rep_agg['Return_Rate']) / 100 * 10)  # 10% weight on low return rate
        )

        rep_agg = rep_agg.sort_values('Efficiency_Score', ascending=False).reset_index(drop=True)

        # Get top performers
        top_reps = rep_agg.head(10).to_dict('records')

        # Sales distribution for chart
        rep_sales_chart = {
            'labels': rep_agg.head(10)['Sales_Rep'].tolist() if len(rep_agg) > 0 else [],
            'sales': rep_agg.head(10)['Total_Sales'].tolist() if len(rep_agg) > 0 else []
        }

        # Invoice counts for chart
        rep_invoice_chart = {
            'labels': rep_agg.head(10)['Sales_Rep'].tolist() if len(rep_agg) > 0 else [],
            'invoices': rep_agg.head(10)['Invoice_Count'].tolist() if len(rep_agg) > 0 else []
        }

        # Efficiency scores for chart
        rep_efficiency_chart = {
            'labels': rep_agg.head(10)['Sales_Rep'].tolist() if len(rep_agg) > 0 else [],
            'scores': rep_agg.head(10)['Efficiency_Score'].tolist() if len(rep_agg) > 0 else []
        }

        # Return rates comparison
        rep_return_chart = {
            'labels': rep_agg['Sales_Rep'].tolist() if len(rep_agg) > 0 else [],
            'return_rates': rep_agg['Return_Rate'].tolist() if len(rep_agg) > 0 else []
        }

        # Performance distribution (high, medium, low performers)
        if len(rep_agg) > 0:
            # Calculate performance tiers
            percentile_75 = rep_agg['Total_Sales'].quantile(0.75)
            percentile_25 = rep_agg['Total_Sales'].quantile(0.25)

            high_performers = len(rep_agg[rep_agg['Total_Sales'] >= percentile_75])
            medium_performers = len(rep_agg[(rep_agg['Total_Sales'] < percentile_75) & (rep_agg['Total_Sales'] > percentile_25)])
            low_performers = len(rep_agg[rep_agg['Total_Sales'] <= percentile_25])

            performance_distribution = {
                'high': safe_int(high_performers),
                'medium': safe_int(medium_performers),
                'low': safe_int(low_performers)
            }
        else:
            performance_distribution = {'high': 0, 'medium': 0, 'low': 0}

        # Summary KPIs
        kpis = {
            'total_reps': len(rep_agg),
            'top_rep': rep_agg.iloc[0]['Sales_Rep'] if len(rep_agg) > 0 else None,
            'top_rep_sales': safe_round(rep_agg.iloc[0]['Total_Sales'], 2) if len(rep_agg) > 0 else 0,
            'top_rep_efficiency': safe_round(rep_agg.iloc[0]['Efficiency_Score'], 2) if len(rep_agg) > 0 else 0,
            'avg_sales_per_rep': safe_round(rep_agg['Total_Sales'].mean(), 2) if len(rep_agg) > 0 else 0,
            'avg_invoices_per_rep': safe_round(rep_agg['Invoice_Count'].mean(), 2) if len(rep_agg) > 0 else 0,
            'avg_customers_per_rep': safe_round(rep_agg['Unique_Customers'].mean(), 2) if len(rep_agg) > 0 else 0,
            'avg_return_rate': safe_round(rep_agg['Return_Rate'].mean(), 2) if len(rep_agg) > 0 else 0,
            'top_performers': top_reps,
            'all_reps': rep_agg.to_dict('records'),
            'sales_chart': rep_sales_chart,
            'invoice_chart': rep_invoice_chart,
            'efficiency_chart': rep_efficiency_chart,
            'return_chart': rep_return_chart,
            'performance_distribution': performance_distribution
        }

        return kpis

    @staticmethod
    def calculate_customer_kpis(line_data: pd.DataFrame,
                                 invoice_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate customer analytics KPIs

        Args:
            line_data: Line-item level data
            invoice_data: Invoice-level data

        Returns:
            Dictionary with customer KPIs
        """
        # Aggregate by customer
        customer_agg = AggregationEngine.aggregate_by_customer(invoice_data, is_invoice_level=True)

        # Calculate customer lifetime value (CLV) using improved model:
        # CLV = Avg_Order_Value * Purchase_Frequency * Retention_Factor
        # Retention_Factor derived from return rate (lower returns = higher retention)
        if len(customer_agg) > 0:
            # Purchase frequency = number of invoices per customer relative to time span
            max_invoices = customer_agg['Invoice_Count'].max()
            customer_agg['Purchase_Frequency'] = customer_agg['Invoice_Count'] / max(max_invoices, 1)

            # Retention factor: (100 - ReturnRate) / 100, with a floor of 0.5
            customer_agg['Retention_Factor'] = ((100 - customer_agg['Return_Rate']) / 100).clip(lower=0.5)

            # CLV = Avg_Invoice_Value * Invoice_Count * Retention_Factor
            customer_agg['CLV_Score'] = (
                customer_agg['Avg_Invoice_Value'] *
                customer_agg['Invoice_Count'] *
                customer_agg['Retention_Factor']
            )
        else:
            customer_agg['CLV_Score'] = 0
            customer_agg['Purchase_Frequency'] = 0
            customer_agg['Retention_Factor'] = 0

        customer_agg = customer_agg.sort_values('CLV_Score', ascending=False).reset_index(drop=True)

        # Customer segments based on sales
        if len(customer_agg) > 0:
            # Calculate Pareto (80/20 rule)
            customer_agg['Cumulative_Sales'] = customer_agg['Total_Sales'].cumsum()
            total_sales = customer_agg['Total_Sales'].sum()
            customer_agg['Cumulative_Pct'] = (customer_agg['Cumulative_Sales'] / total_sales * 100)

            # Segment customers
            customer_agg['Segment'] = 'Bronze'
            customer_agg.loc[customer_agg['Cumulative_Pct'] <= 80, 'Segment'] = 'Gold'
            customer_agg.loc[customer_agg['Cumulative_Pct'] <= 50, 'Segment'] = 'Platinum'

            # Count segments
            segment_counts = customer_agg['Segment'].value_counts().to_dict()
        else:
            segment_counts = {}

        # Get top customers
        top_customers = customer_agg.head(10).to_dict('records')

        # Summary KPIs
        kpis = {
            'total_customers': len(customer_agg),
            'top_customer': customer_agg.iloc[0]['Customer'] if len(customer_agg) > 0 else None,
            'top_customer_sales': float(customer_agg.iloc[0]['Total_Sales']) if len(customer_agg) > 0 else 0,
            'avg_sales_per_customer': float(customer_agg['Total_Sales'].mean()) if len(customer_agg) > 0 else 0,
            'avg_invoices_per_customer': float(customer_agg['Invoice_Count'].mean()) if len(customer_agg) > 0 else 0,
            'avg_customer_return_rate': float(customer_agg['Return_Rate'].mean()) if len(customer_agg) > 0 else 0,
            'segment_distribution': segment_counts,
            'top_customers': top_customers,
            'all_customers': customer_agg.to_dict('records')
        }

        return kpis

    @staticmethod
    def calculate_product_kpis(line_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate product/production line KPIs with detailed material group analysis

        Args:
            line_data: Line-item level data

        Returns:
            Dictionary with product KPIs including material groups, sizes, and items
        """
        # Aggregate by production line
        line_agg = AggregationEngine.aggregate_by_product_line(line_data, is_invoice_level=False)

        # Calculate contribution percentages
        total_sales = line_agg['Total_Sales'].sum()
        line_agg['Contribution_Pct'] = line_agg['Total_Sales'].apply(
            lambda x: calculate_percentage(x, total_sales)
        )

        # Get top lines
        top_lines = line_agg.head(10).to_dict('records')

        # Enhanced Material Group Analysis
        material_groups_data = KPIEngine._analyze_material_groups(line_data, total_sales)

        # Enhanced Item Analysis (Top items with details)
        items_data = KPIEngine._analyze_items(line_data, total_sales)

        # Size Analysis
        sizes_data = KPIEngine._analyze_sizes(line_data, total_sales)

        # Summary KPIs
        kpis = {
            'total_product_lines': len(line_agg),
            'top_line': line_agg.iloc[0]['Production_Line'] if len(line_agg) > 0 else None,
            'top_line_sales': float(line_agg.iloc[0]['Total_Sales']) if len(line_agg) > 0 else 0,
            'top_line_contribution': float(line_agg.iloc[0]['Contribution_Pct']) if len(line_agg) > 0 else 0,
            'avg_sales_per_line': float(line_agg['Total_Sales'].mean()) if len(line_agg) > 0 else 0,
            'top_lines': top_lines,
            'all_lines': line_agg.to_dict('records'),

            # Enhanced material analysis
            'material_groups': material_groups_data,
            'items_analysis': items_data,
            'sizes_analysis': sizes_data
        }

        return kpis

    @staticmethod
    def _extract_material_group(item_name: str) -> str:
        """
        Extract material group from item name

        Args:
            item_name: Item name string

        Returns:
            Material group name
        """
        if not isinstance(item_name, str):
            return 'Other'

        item_lower = item_name.lower().strip()

        # Define material group patterns
        if 'interlock' in item_lower:
            return 'Interlock'
        elif 'curbstone' in item_lower or 'curb' in item_lower:
            return 'Curbstone'
        elif 'cement tiles' in item_lower or 'tiles' in item_lower:
            return 'Cement Tiles'
        elif 'cbl tiles' in item_lower or 'cbl' in item_lower:
            return 'CBL Tiles'
        elif 'paver' in item_lower or 'pavers' in item_lower:
            return 'Pavers'
        elif 'block' in item_lower:
            return 'Blocks'
        elif 'charge' in item_lower:
            return 'Charges'
        else:
            return 'Other'

    @staticmethod
    def _extract_size(item_name: str) -> str:
        """
        Extract size/thickness from item name

        Args:
            item_name: Item name string

        Returns:
            Size string
        """
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

        # Try to find patterns like "40*40*4"
        dim_match2 = re.search(r'(\d+\*\d+\*\d+)', item_name)
        if dim_match2:
            return dim_match2.group(1)

        return 'N/A'

    @staticmethod
    def _extract_color(item_name: str) -> str:
        """
        Extract color from item name

        Args:
            item_name: Item name string

        Returns:
            Color string
        """
        if not isinstance(item_name, str):
            return 'N/A'

        item_lower = item_name.lower().strip()

        # Common colors
        colors = ['grey', 'red', 'orange', 'yellow', 'green', 'blue', 'black', 'white', 'brown']

        for color in colors:
            if color in item_lower:
                return color.capitalize()

        return 'N/A'

    @staticmethod
    def _analyze_material_groups(line_data: pd.DataFrame, total_sales: float) -> Dict[str, Any]:
        """
        Analyze sales by material groups (Interlock, Curbstone, etc.)

        Args:
            line_data: Line-item level data
            total_sales: Total sales amount

        Returns:
            Dictionary with material group analysis
        """
        # Extract material groups
        line_data_copy = line_data.copy()
        line_data_copy['Material_Group'] = line_data_copy['Item'].apply(
            KPIEngine._extract_material_group
        )

        # Aggregate by material group
        group_agg = line_data_copy.groupby('Material_Group').agg({
            'Total Amount': 'sum',
            'Quantity': 'sum',
            'Item': 'count',
            'Customer': 'nunique'
        }).reset_index()

        group_agg.columns = ['Material_Group', 'Total_Sales', 'Total_Quantity',
                            'Line_Item_Count', 'Unique_Customers']

        # Calculate contribution percentages
        group_agg['Contribution_Pct'] = group_agg['Total_Sales'].apply(
            lambda x: calculate_percentage(x, total_sales)
        )

        # Calculate average values
        group_agg['Avg_Item_Value'] = group_agg.apply(
            lambda row: safe_divide(row['Total_Sales'], row['Line_Item_Count']),
            axis=1
        )

        # Sort by sales
        group_agg = group_agg.sort_values('Total_Sales', ascending=False).reset_index(drop=True)

        # Get top group
        top_group = group_agg.iloc[0]['Material_Group'] if len(group_agg) > 0 else None
        top_group_sales = float(group_agg.iloc[0]['Total_Sales']) if len(group_agg) > 0 else 0
        top_group_contribution = float(group_agg.iloc[0]['Contribution_Pct']) if len(group_agg) > 0 else 0

        return {
            'total_groups': len(group_agg),
            'top_group': top_group,
            'top_group_sales': top_group_sales,
            'top_group_contribution': top_group_contribution,
            'all_groups': group_agg.to_dict('records'),
            'chart_labels': group_agg['Material_Group'].tolist(),
            'chart_values': group_agg['Total_Sales'].tolist()
        }

    @staticmethod
    def _analyze_items(line_data: pd.DataFrame, total_sales: float) -> Dict[str, Any]:
        """
        Analyze individual items with detailed breakdown

        Args:
            line_data: Line-item level data
            total_sales: Total sales amount

        Returns:
            Dictionary with items analysis
        """
        # Aggregate by item
        item_agg = line_data.groupby('Item').agg({
            'Total Amount': 'sum',
            'Quantity': 'sum',
            'Customer': 'nunique'
        }).reset_index()

        item_agg.columns = ['Item', 'Total_Sales', 'Total_Quantity', 'Unique_Customers']

        # Calculate contribution percentages
        item_agg['Contribution_Pct'] = item_agg['Total_Sales'].apply(
            lambda x: calculate_percentage(x, total_sales)
        )

        # Extract additional info
        item_agg['Material_Group'] = item_agg['Item'].apply(KPIEngine._extract_material_group)
        item_agg['Size'] = item_agg['Item'].apply(KPIEngine._extract_size)
        item_agg['Color'] = item_agg['Item'].apply(KPIEngine._extract_color)

        # Sort by sales
        item_agg = item_agg.sort_values('Total_Sales', ascending=False).reset_index(drop=True)

        # Get top 20 items
        top_items = item_agg.head(20).to_dict('records')

        return {
            'total_items': len(item_agg),
            'top_items': top_items,
            'all_items': item_agg.to_dict('records')[:50]  # Limit to top 50 for performance
        }

    @staticmethod
    def _analyze_sizes(line_data: pd.DataFrame, total_sales: float) -> Dict[str, Any]:
        """
        Analyze sales by product size

        Args:
            line_data: Line-item level data
            total_sales: Total sales amount

        Returns:
            Dictionary with size analysis
        """
        # Extract sizes
        line_data_copy = line_data.copy()
        line_data_copy['Size'] = line_data_copy['Item'].apply(KPIEngine._extract_size)

        # Filter out N/A sizes
        sized_data = line_data_copy[line_data_copy['Size'] != 'N/A']

        if len(sized_data) == 0:
            return {
                'total_sizes': 0,
                'all_sizes': [],
                'chart_labels': [],
                'chart_values': []
            }

        # Aggregate by size
        size_agg = sized_data.groupby('Size').agg({
            'Total Amount': 'sum',
            'Quantity': 'sum',
            'Item': 'count'
        }).reset_index()

        size_agg.columns = ['Size', 'Total_Sales', 'Total_Quantity', 'Line_Item_Count']

        # Calculate contribution percentages
        size_agg['Contribution_Pct'] = size_agg['Total_Sales'].apply(
            lambda x: calculate_percentage(x, total_sales)
        )

        # Sort by sales
        size_agg = size_agg.sort_values('Total_Sales', ascending=False).reset_index(drop=True)

        return {
            'total_sizes': len(size_agg),
            'all_sizes': size_agg.to_dict('records'),
            'chart_labels': size_agg['Size'].tolist()[:10],  # Top 10 for chart
            'chart_values': size_agg['Total_Sales'].tolist()[:10]
        }

    @staticmethod
    def calculate_time_trends(line_data: pd.DataFrame,
                               invoice_data: pd.DataFrame,
                               period: str = 'Month') -> Dict[str, Any]:
        """
        Calculate time-based trends

        Args:
            line_data: Line-item level data
            invoice_data: Invoice-level data
            period: 'Year', 'Quarter', 'Month', 'Week', 'Day'

        Returns:
            Dictionary with trend data
        """
        # Aggregate by time period
        time_agg = AggregationEngine.aggregate_by_time(
            invoice_data, period, is_invoice_level=True
        )

        # Calculate period-over-period changes
        time_comp = AggregationEngine.calculate_period_comparison(
            invoice_data, period, is_invoice_level=True
        )

        # Calculate returns by period
        if 'Is_Return' in invoice_data.columns:
            returns_by_period = invoice_data[invoice_data['Is_Return']].groupby(period).agg({
                'Total Amount': 'sum'
            }).reset_index()
            returns_by_period.columns = [period, 'Return_Amount']
            time_agg = time_agg.merge(returns_by_period, on=period, how='left')
            time_agg['Return_Amount'] = time_agg['Return_Amount'].fillna(0)
        else:
            time_agg['Return_Amount'] = 0

        # Format for charts
        trends = {
            'period_type': period,
            'periods': time_agg[period].tolist(),
            'total_sales': time_agg['Total Amount'].tolist(),
            'total_returns': time_agg['Return_Amount'].tolist(),
            'invoice_counts': time_agg['Invoice_Count'].tolist(),
            'avg_invoice_values': time_agg['Avg_Invoice_Value'].tolist(),
            'growth_rates': time_comp['Total_Amount_Pct_Change'].fillna(0).tolist()
        }

        return trends

    @staticmethod
    def calculate_all_kpis(line_data: pd.DataFrame,
                            invoice_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate all KPIs at once

        Args:
            line_data: Line-item level data
            invoice_data: Invoice-level data

        Returns:
            Dictionary with all KPI categories
        """
        trends_monthly = KPIEngine.calculate_time_trends(line_data, invoice_data, 'Month')

        # Calculate latest period-over-period changes for KPI trend indicators
        growth_rates = trends_monthly.get('growth_rates', [])
        sales_list = trends_monthly.get('total_sales', [])
        invoices_list = trends_monthly.get('invoice_counts', [])

        period_changes = {}
        if len(growth_rates) >= 2:
            period_changes['sales_change_pct'] = safe_round(growth_rates[-1], 2)
        if len(invoices_list) >= 2 and invoices_list[-2] != 0:
            period_changes['invoices_change_pct'] = safe_round(
                ((invoices_list[-1] - invoices_list[-2]) / invoices_list[-2]) * 100, 2
            )

        all_kpis = {
            'overview': KPIEngine.calculate_overview_kpis(line_data, invoice_data),
            'sales': KPIEngine.calculate_sales_kpis(line_data, invoice_data),
            'representatives': KPIEngine.calculate_rep_kpis(line_data, invoice_data),
            'customers': KPIEngine.calculate_customer_kpis(line_data, invoice_data),
            'products': KPIEngine.calculate_product_kpis(line_data),
            'trends_monthly': trends_monthly,
            'trends_quarterly': KPIEngine.calculate_time_trends(line_data, invoice_data, 'Quarter'),
            'period_changes': period_changes
        }

        return all_kpis
