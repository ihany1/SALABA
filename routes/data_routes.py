"""
Data Routes - Handle data upload, validation, and filtering
Blueprint: /api/data
"""

from flask import Blueprint, request, jsonify, current_app
from werkzeug.utils import secure_filename
import os
from pathlib import Path
import sys

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).resolve().parent.parent))

from services.data_ingestion import DataIngestionService
from services.data_manager import get_data_manager
from services.filtering_engine import FilteringEngine
from utils.validators import allowed_file

data_bp = Blueprint('data', __name__)


@data_bp.route('/upload', methods=['POST'])
def upload_file():
    """
    Upload and process Excel file
    ---
    Endpoint: POST /api/data/upload
    Returns: Upload status and data summary
    """
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400

        file = request.files['file']

        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400

        # Check file extension
        allowed_extensions = current_app.config.get('ALLOWED_EXTENSIONS', {'xlsx', 'xls'})
        if not allowed_file(file.filename, allowed_extensions):
            return jsonify({
                'error': f'Invalid file type. Allowed types: {", ".join(allowed_extensions)}'
            }), 400

        # Secure the filename
        filename = secure_filename(file.filename)

        # Save file to upload folder
        upload_folder = current_app.config.get('UPLOAD_FOLDER')
        os.makedirs(upload_folder, exist_ok=True)
        file_path = os.path.join(upload_folder, filename)
        file.save(file_path)

        # Process the file
        ingestion_service = DataIngestionService()
        success, result = ingestion_service.upload_file(file_path)

        if not success:
            # Clean up file on failure
            if os.path.exists(file_path):
                os.remove(file_path)
            return jsonify(result), 400

        # Store data in data manager
        data_manager = get_data_manager()
        data_manager.set_data(
            line_item_data=ingestion_service.get_data(),
            invoice_data=ingestion_service.get_invoice_data(),
            metadata=ingestion_service.get_metadata()
        )
        data_manager.set_source_filename(filename)

        # Return success with metadata
        return jsonify({
            'success': True,
            'message': 'File uploaded and processed successfully',
            'filename': filename,
            'metadata': result['metadata'],
            'validation': result['validation_report']
        }), 200

    except Exception as e:
        return jsonify({'error': f'Upload failed: {str(e)}'}), 500


@data_bp.route('/summary', methods=['GET'])
def get_data_summary():
    """
    Get summary of loaded data
    ---
    Endpoint: GET /api/data/summary
    Returns: Dataset statistics and metadata
    """
    try:
        data_manager = get_data_manager()

        if not data_manager.is_data_loaded():
            return jsonify({
                'error': 'No data loaded',
                'message': 'Please upload a file first'
            }), 404

        summary = data_manager.get_summary_stats()
        metadata = data_manager.get_metadata()
        freshness = data_manager.get_freshness_info()

        return jsonify({
            'success': True,
            'summary': summary,
            'metadata': metadata,
            'freshness': freshness
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@data_bp.route('/filter', methods=['POST'])
def apply_filters():
    """
    Apply filters to dataset and return results
    ---
    Endpoint: POST /api/data/filter
    Body: {
        "filters": {
            "year": [2023, 2024],
            "sales_rep": ["Rep A", "Rep B"],
            "customer": ["Customer X"],
            ...
        },
        "section": "overview" | "sales" | "reps" | etc.
    }
    Returns: Filtered and aggregated data for requested section
    """
    try:
        data_manager = get_data_manager()

        if not data_manager.is_data_loaded():
            return jsonify({
                'error': 'No data loaded',
                'message': 'Please upload a file first'
            }), 404

        filters = request.json.get('filters', {}) if request.json else {}
        section = request.json.get('section', 'overview') if request.json else 'overview'

        # Validate filters
        is_valid, errors = FilteringEngine.validate_filters(filters)
        if not is_valid:
            return jsonify({
                'error': 'Invalid filters',
                'validation_errors': errors
            }), 400

        # Get appropriate dataset (line-item or invoice level depends on section)
        line_data = data_manager.get_line_item_data()
        invoice_data = data_manager.get_invoice_data()

        # Apply filters to both datasets
        filtered_line_data = FilteringEngine.apply_filters(line_data, filters)
        filtered_invoice_data = FilteringEngine.apply_filters(invoice_data, filters)

        # Get filter summary
        filter_summary = FilteringEngine.get_filter_summary(filters)
        filter_description = FilteringEngine.build_filter_description(filters)

        return jsonify({
            'success': True,
            'section': section,
            'filter_summary': filter_summary,
            'filter_description': filter_description,
            'result_counts': {
                'line_items': len(filtered_line_data),
                'invoices': len(filtered_invoice_data)
            },
            'message': 'Filters applied successfully'
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@data_bp.route('/filters/options', methods=['GET'])
def get_filter_options():
    """
    Get available filter options from current dataset
    ---
    Endpoint: GET /api/data/filters/options
    Returns: All unique values for each filterable dimension
    """
    try:
        data_manager = get_data_manager()

        if not data_manager.is_data_loaded():
            return jsonify({
                'error': 'No data loaded',
                'message': 'Please upload a file first'
            }), 404

        options = data_manager.get_filter_options()
        date_range = data_manager.get_date_range()

        return jsonify({
            'success': True,
            'options': options,
            'date_range': date_range
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@data_bp.route('/validate', methods=['POST'])
def validate_data():
    """
    Validate uploaded data structure
    ---
    Endpoint: POST /api/data/validate
    Returns: Validation results and any errors
    """
    try:
        # This will validate required columns, data types, etc.
        return jsonify({
            'message': 'Validation endpoint ready',
            'status': 'pending_implementation'
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500
