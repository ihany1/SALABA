"""
Sales Intelligence Application - Main Flask Application
Production-ready backend with modular architecture
"""

from flask import Flask, render_template, jsonify
from flask_cors import CORS
from config import get_config
import os


def create_app(config_name=None):
    """
    Application factory pattern
    Creates and configures the Flask application
    """
    _backend_dir = os.path.dirname(os.path.abspath(__file__))
    _root_dir = os.path.dirname(_backend_dir)

    # Templates/static are copied next to app.py during Railway build.
    # Fall back to the repo-root frontend/ layout for local development.
    _template_dir = os.path.join(_backend_dir, 'templates')
    _static_dir   = os.path.join(_backend_dir, 'static')
    if not os.path.isdir(_template_dir):
        _template_dir = os.path.join(_root_dir, 'frontend', 'templates')
        _static_dir   = os.path.join(_root_dir, 'frontend', 'static')

    app = Flask(
        __name__,
        template_folder=_template_dir,
        static_folder=_static_dir
    )

    # Load configuration
    config = get_config(config_name)
    app.config.from_object(config)
    config.init_app(app)

    # Enable CORS for API endpoints
    CORS(app, resources={r"/api/*": {"origins": "*"}})

    # Register blueprints (API routes)
    register_blueprints(app)

    # Register error handlers
    register_error_handlers(app)

    # Main route - serve the app
    @app.route('/')
    def index():
        """Serve the main application page"""
        return render_template('index.html')

    # Health check endpoint
    @app.route('/health')
    def health():
        """Health check endpoint for monitoring"""
        return jsonify({
            'status': 'healthy',
            'service': 'Sales Intelligence API',
            'version': '1.0.0'
        }), 200

    return app


def register_blueprints(app):
    """Register all API blueprints"""
    from routes.data_routes import data_bp
    from routes.analytics_routes import analytics_bp
    from routes.export_routes import export_bp
    from routes.ml_routes import ml_bp

    app.register_blueprint(data_bp, url_prefix='/api/data')
    app.register_blueprint(analytics_bp, url_prefix='/api/analytics')
    app.register_blueprint(export_bp, url_prefix='/api/export')
    app.register_blueprint(ml_bp, url_prefix='/api/ml')


def register_error_handlers(app):
    """Register custom error handlers"""

    @app.errorhandler(404)
    def not_found(error):
        return jsonify({
            'error': 'Not Found',
            'message': 'The requested resource was not found'
        }), 404

    @app.errorhandler(500)
    def internal_error(error):
        return jsonify({
            'error': 'Internal Server Error',
            'message': 'An internal error occurred'
        }), 500

    @app.errorhandler(400)
    def bad_request(error):
        return jsonify({
            'error': 'Bad Request',
            'message': 'The request was invalid'
        }), 400

    @app.errorhandler(413)
    def request_entity_too_large(error):
        return jsonify({
            'error': 'File Too Large',
            'message': 'The uploaded file exceeds the maximum allowed size'
        }), 413


if __name__ == '__main__':
    # Create and run the application
    app = create_app()

    # Get host and port from config
    host = app.config.get('HOST', '0.0.0.0')
    port = app.config.get('PORT', 5000)
    debug = app.config.get('DEBUG', True)

    print(f"""
    ========================================
      Sales Intelligence Application
      Running on: http://{host}:{port}
      Debug Mode: {debug}
    ========================================
    """)

    app.run(host=host, port=port, debug=debug)
