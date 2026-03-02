"""
Configuration module for Sales Intelligence Application
Loads environment variables and provides application settings
"""

import os
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
load_dotenv()

# Base directory
BASE_DIR = Path(__file__).resolve().parent.parent


class Config:
    """Base configuration class"""

    # Flask Settings
    SECRET_KEY = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')
    DEBUG = os.getenv('DEBUG', 'True').lower() == 'true'

    # Server Settings
    HOST = os.getenv('HOST', '0.0.0.0')
    PORT = int(os.getenv('PORT', 5000))

    # Upload Settings
    UPLOAD_FOLDER = BASE_DIR / os.getenv('UPLOAD_FOLDER', 'uploads')
    MAX_CONTENT_LENGTH = int(os.getenv('MAX_CONTENT_LENGTH', 16 * 1024 * 1024))  # 16MB
    ALLOWED_EXTENSIONS = set(os.getenv('ALLOWED_EXTENSIONS', 'xlsx,xls').split(','))

    # Export Settings
    EXPORT_FOLDER = BASE_DIR / os.getenv('EXPORT_FOLDER', 'exports')
    PDF_TEMPLATE_PATH = BASE_DIR / os.getenv('PDF_TEMPLATE_PATH', 'frontend/templates/pdf')

    # Database Settings (PostgreSQL Ready)
    DATABASE_URL = os.getenv('DATABASE_URL', '')
    USE_DATABASE = os.getenv('USE_DATABASE', 'False').lower() == 'true'

    # ML Settings
    ML_ENABLED = os.getenv('ML_ENABLED', 'False').lower() == 'true'
    ML_MODEL_PATH = BASE_DIR / os.getenv('ML_MODEL_PATH', 'backend/models/ml_models')

    # Performance Settings
    CACHE_ENABLED = os.getenv('CACHE_ENABLED', 'True').lower() == 'true'
    CACHE_TIMEOUT = int(os.getenv('CACHE_TIMEOUT', 300))

    # Data Processing Settings
    CHUNK_SIZE = 10000  # For processing large files
    DEFAULT_DATE_FORMAT = '%Y-%m-%d'

    # Invoice Aggregation Settings
    INVOICE_ID_COLUMN = 'Inv#'
    DATE_COLUMN = 'Date'
    AMOUNT_COLUMN = 'Total Amount'
    CUSTOMER_COLUMN = 'Customer'
    REP_COLUMN = 'Sales Rep'
    PRODUCT_LINE_COLUMN = 'Production Line'

    @staticmethod
    def init_app(app):
        """Initialize application with config"""
        # Create necessary directories
        os.makedirs(Config.UPLOAD_FOLDER, exist_ok=True)
        os.makedirs(Config.EXPORT_FOLDER, exist_ok=True)
        if Config.ML_ENABLED:
            os.makedirs(Config.ML_MODEL_PATH, exist_ok=True)


class DevelopmentConfig(Config):
    """Development configuration"""
    DEBUG = True
    TESTING = False


class ProductionConfig(Config):
    """Production configuration"""
    DEBUG = False
    TESTING = False

    # Override with stronger security in production
    @classmethod
    def init_app(cls, app):
        Config.init_app(app)

        # Production-specific initialization
        import logging
        from logging.handlers import RotatingFileHandler

        if not app.debug:
            file_handler = RotatingFileHandler(
                'logs/sales_intelligence.log',
                maxBytes=10240000,
                backupCount=10
            )
            file_handler.setFormatter(logging.Formatter(
                '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
            ))
            file_handler.setLevel(logging.INFO)
            app.logger.addHandler(file_handler)

            app.logger.setLevel(logging.INFO)
            app.logger.info('Sales Intelligence startup')


class TestingConfig(Config):
    """Testing configuration"""
    TESTING = True
    DEBUG = True
    UPLOAD_FOLDER = BASE_DIR / 'test_uploads'
    EXPORT_FOLDER = BASE_DIR / 'test_exports'


# Configuration dictionary
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}


def get_config(config_name=None):
    """Get configuration by name"""
    if config_name is None:
        config_name = os.getenv('FLASK_ENV', 'development')
    return config.get(config_name, config['default'])
