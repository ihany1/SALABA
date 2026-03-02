"""
Analytics Routes - Handle KPIs and analytics calculations
Blueprint: /api/analytics
"""

from flask import Blueprint, request, jsonify
from pathlib import Path
import sys

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).resolve().parent.parent))

from services.data_manager import get_data_manager
from services.filtering_engine import FilteringEngine
from services.kpi_engine import KPIEngine
from services.aggregation_engine import AggregationEngine
from utils.helpers import sanitize_for_json

analytics_bp = Blueprint('analytics', __name__)


@analytics_bp.route('/kpis', methods=['POST'])
def get_kpis():
    """
    Get all KPIs with optional filters
    ---
    Endpoint: POST /api/analytics/kpis
    Body: { "filters": {...} }
    Returns: All calculated KPIs (invoice-level aware)
    """
    try:
        data_manager = get_data_manager()

        if not data_manager.is_data_loaded():
            return jsonify({
                'error': 'No data loaded',
                'message': 'Please upload a file first'
            }), 404

        filters = request.json.get('filters', {}) if request.json else {}

        # Get data
        line_data = data_manager.get_line_item_data()
        invoice_data = data_manager.get_invoice_data()

        # Apply filters if provided
        if filters:
            line_data = FilteringEngine.apply_filters(line_data, filters)
            invoice_data = FilteringEngine.apply_filters(invoice_data, filters)

        # Calculate all KPIs
        all_kpis = KPIEngine.calculate_all_kpis(line_data, invoice_data)

        # Add filter info
        filter_summary = FilteringEngine.get_filter_summary(filters)

        # Sanitize for JSON serialization (convert numpy/pandas types)
        all_kpis_sanitized = sanitize_for_json(all_kpis)
        filter_summary_sanitized = sanitize_for_json(filter_summary)

        return jsonify({
            'success': True,
            'kpis': all_kpis_sanitized,
            'filter_summary': filter_summary_sanitized
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@analytics_bp.route('/sales', methods=['POST'])
def get_sales_analytics():
    """
    Get detailed sales analytics
    ---
    Endpoint: POST /api/analytics/sales
    Returns: Sales trends, breakdowns, and insights
    """
    try:
        filters = request.json.get('filters', {}) if request.json else {}

        return jsonify({
            'message': 'Sales analytics endpoint ready',
            'status': 'pending_implementation'
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@analytics_bp.route('/reps', methods=['POST'])
def get_rep_analytics():
    """
    Get representative performance analytics
    ---
    Endpoint: POST /api/analytics/reps
    Returns: Rep rankings, efficiency metrics, trends
    """
    try:
        filters = request.json.get('filters', {}) if request.json else {}

        return jsonify({
            'message': 'Rep analytics endpoint ready',
            'status': 'pending_implementation'
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@analytics_bp.route('/customers', methods=['POST'])
def get_customer_analytics():
    """
    Get customer analytics and segmentation
    ---
    Endpoint: POST /api/analytics/customers
    Returns: Customer rankings, behavior, segments
    """
    try:
        filters = request.json.get('filters', {}) if request.json else {}

        return jsonify({
            'message': 'Customer analytics endpoint ready',
            'status': 'pending_implementation'
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@analytics_bp.route('/products', methods=['POST'])
def get_product_analytics():
    """
    Get product/production line analytics
    ---
    Endpoint: POST /api/analytics/products
    Returns: Product performance, line analysis
    """
    try:
        filters = request.json.get('filters', {}) if request.json else {}

        return jsonify({
            'message': 'Product analytics endpoint ready',
            'status': 'pending_implementation'
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@analytics_bp.route('/trends', methods=['POST'])
def get_trends():
    """
    Get time-based trends and patterns
    ---
    Endpoint: POST /api/analytics/trends
    Returns: Time series data for various metrics
    """
    try:
        filters = request.json.get('filters', {}) if request.json else {}
        metric = request.json.get('metric', 'sales') if request.json else 'sales'

        return jsonify({
            'message': 'Trends endpoint ready',
            'metric': metric,
            'status': 'pending_implementation'
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@analytics_bp.route('/compare-periods', methods=['POST'])
def compare_two_periods():
    """
    Compare two custom date ranges
    ---
    Endpoint: POST /api/analytics/compare-periods
    Body: {
        "period1": {"start": "2024-01-01", "end": "2024-03-31"},
        "period2": {"start": "2024-04-01", "end": "2024-06-30"},
        "filters": {...} (optional)
    }
    Returns: Comprehensive comparison between two periods
    """
    try:
        data_manager = get_data_manager()

        if not data_manager.is_data_loaded():
            return jsonify({
                'error': 'No data loaded',
                'message': 'Please upload a file first'
            }), 404

        # Get period definitions
        if not request.json:
            return jsonify({'error': 'Request body required'}), 400

        period1 = request.json.get('period1')
        period2 = request.json.get('period2')
        base_filters = request.json.get('filters', {})

        if not period1 or not period2:
            return jsonify({
                'error': 'Both period1 and period2 are required',
                'example': {
                    'period1': {'start': '2024-01-01', 'end': '2024-03-31'},
                    'period2': {'start': '2024-04-01', 'end': '2024-06-30'}
                }
            }), 400

        # Validate date formats
        if not ('start' in period1 and 'end' in period1):
            return jsonify({'error': 'period1 must have start and end dates'}), 400
        if not ('start' in period2 and 'end' in period2):
            return jsonify({'error': 'period2 must have start and end dates'}), 400

        # Get data
        line_data = data_manager.get_line_item_data()
        invoice_data = data_manager.get_invoice_data()

        # Apply base filters (if any)
        if base_filters:
            line_data = FilteringEngine.apply_filters(line_data, base_filters)
            invoice_data = FilteringEngine.apply_filters(invoice_data, base_filters)

        # Filter for Period 1
        period1_filters = {'date_start': period1['start'], 'date_end': period1['end']}
        period1_line = FilteringEngine.apply_filters(line_data, period1_filters)
        period1_invoice = FilteringEngine.apply_filters(invoice_data, period1_filters)

        # Filter for Period 2
        period2_filters = {'date_start': period2['start'], 'date_end': period2['end']}
        period2_line = FilteringEngine.apply_filters(line_data, period2_filters)
        period2_invoice = FilteringEngine.apply_filters(invoice_data, period2_filters)

        # Calculate KPIs for both periods
        period1_kpis = KPIEngine.calculate_all_kpis(period1_line, period1_invoice)
        period2_kpis = KPIEngine.calculate_all_kpis(period2_line, period2_invoice)

        # Calculate deltas
        comparison = {
            'period1': {
                'label': f"{period1['start']} to {period1['end']}",
                'dates': period1,
                'kpis': period1_kpis
            },
            'period2': {
                'label': f"{period2['start']} to {period2['end']}",
                'dates': period2,
                'kpis': period2_kpis
            },
            'comparison': {
                'sales': {
                    'period1': float(period1_kpis['sales']['gross_sales']),
                    'period2': float(period2_kpis['sales']['gross_sales']),
                    'change': float(period2_kpis['sales']['gross_sales'] - period1_kpis['sales']['gross_sales']),
                    'change_percent': float((period2_kpis['sales']['gross_sales'] - period1_kpis['sales']['gross_sales']) / period1_kpis['sales']['gross_sales'] * 100) if period1_kpis['sales']['gross_sales'] > 0 else 0
                },
                'returns': {
                    'period1': float(period1_kpis['sales']['total_returns']),
                    'period2': float(period2_kpis['sales']['total_returns']),
                    'change': float(period2_kpis['sales']['total_returns'] - period1_kpis['sales']['total_returns']),
                    'change_percent': float((period2_kpis['sales']['total_returns'] - period1_kpis['sales']['total_returns']) / abs(period1_kpis['sales']['total_returns']) * 100) if period1_kpis['sales']['total_returns'] != 0 else 0
                },
                'net_sales': {
                    'period1': float(period1_kpis['sales']['net_sales']),
                    'period2': float(period2_kpis['sales']['net_sales']),
                    'change': float(period2_kpis['sales']['net_sales'] - period1_kpis['sales']['net_sales']),
                    'change_percent': float((period2_kpis['sales']['net_sales'] - period1_kpis['sales']['net_sales']) / period1_kpis['sales']['net_sales'] * 100) if period1_kpis['sales']['net_sales'] > 0 else 0
                },
                'invoices': {
                    'period1': int(period1_kpis['sales']['sales_invoice_count']),
                    'period2': int(period2_kpis['sales']['sales_invoice_count']),
                    'change': int(period2_kpis['sales']['sales_invoice_count'] - period1_kpis['sales']['sales_invoice_count']),
                    'change_percent': float((period2_kpis['sales']['sales_invoice_count'] - period1_kpis['sales']['sales_invoice_count']) / period1_kpis['sales']['sales_invoice_count'] * 100) if period1_kpis['sales']['sales_invoice_count'] > 0 else 0
                },
                'avg_invoice_value': {
                    'period1': float(period1_kpis['sales']['avg_invoice_value']),
                    'period2': float(period2_kpis['sales']['avg_invoice_value']),
                    'change': float(period2_kpis['sales']['avg_invoice_value'] - period1_kpis['sales']['avg_invoice_value']),
                    'change_percent': float((period2_kpis['sales']['avg_invoice_value'] - period1_kpis['sales']['avg_invoice_value']) / period1_kpis['sales']['avg_invoice_value'] * 100) if period1_kpis['sales']['avg_invoice_value'] > 0 else 0
                },
                'return_rate': {
                    'period1': float(period1_kpis['sales']['return_rate']),
                    'period2': float(period2_kpis['sales']['return_rate']),
                    'change': float(period2_kpis['sales']['return_rate'] - period1_kpis['sales']['return_rate']),
                    'change_percent': 0
                },
                'customers': {
                    'period1': int(period1_kpis['customers']['total_customers']),
                    'period2': int(period2_kpis['customers']['total_customers']),
                    'change': int(period2_kpis['customers']['total_customers'] - period1_kpis['customers']['total_customers']),
                    'change_percent': float((period2_kpis['customers']['total_customers'] - period1_kpis['customers']['total_customers']) / period1_kpis['customers']['total_customers'] * 100) if period1_kpis['customers']['total_customers'] > 0 else 0
                }
            }
        }

        # Sanitize for JSON
        comparison_sanitized = sanitize_for_json(comparison)

        return jsonify({
            'success': True,
            'comparison': comparison_sanitized
        }), 200

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@analytics_bp.route('/comparison', methods=['POST'])
def get_period_comparison():
    """
    Legacy endpoint - redirects to compare-periods
    ---
    Endpoint: POST /api/analytics/comparison
    Returns: Period-over-period comparisons
    """
    try:
        filters = request.json.get('filters', {}) if request.json else {}
        period_type = request.json.get('period_type', 'month') if request.json else 'month'

        return jsonify({
            'message': 'Period comparison endpoint ready',
            'period_type': period_type,
            'note': 'Use /api/analytics/compare-periods for two custom date ranges',
            'status': 'pending_implementation'
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500
