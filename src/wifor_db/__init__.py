"""
Package Description: This package includes modules for managing and
interacting with the wifor_platform database.
"""

__version__ = '1.0.0'

from .class_handler import create_class
from .env_loader import get_env, _env_cache
from .wifor_logger import open_log, close_log

# Call the get_env function to load the environmental variables
get_env()

# Add the _env_cache variable to the __all__ list
__all__ = ['_env_cache']
