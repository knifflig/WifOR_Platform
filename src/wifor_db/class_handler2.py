# Standard library imports
import os
import json
import traceback
from datetime import datetime

# Third-party imports
import sqlalchemy
from sqlalchemy import Column, Integer, Date
from sqlalchemy.ext.declarative import declarative_base

# Local application imports
from wifor_db import _env_cache, open_log, close_log

class JsonLoaderError(Exception):
    """ Custom exception class for JSON Loader errors. """
    pass

def load_class_json(class_name: str) -> dict:
    """
    Load JSON data for a given class.

    Args:
        class_name (str): The name of the class for which the JSON is to be loaded.

    Returns:
        dict: The JSON content of the class.

    Raises:
        JsonLoaderError: If the JSON file cannot be found or read.
    """
    log = open_log(class_name)
    log.info("Starting to load JSON for class: %s", class_name)

    json_path = os.path.join(_env_cache['CLASS_DIR'], f"{class_name}.json")
    log.info("""Attempting to load JSON file from path:
                %s""", json_path)

    if not os.path.exists(json_path):
        log.error("The file %s does not exist.", json_path)
        close_log(log)
        raise JsonLoaderError(f"The file {json_path} does not exist.")
    
    try:
        with open(json_path, 'r', encoding="utf-8") as file:
            class_schema = json.load(file)
            log.info("JSON data successfully loaded for class: %s", class_name)
    except Exception as e:
        log.error("Error loading JSON file: %s\n%s", e, traceback.format_exc())
        close_log(log)
        raise JsonLoaderError(f"Error loading JSON file: {e}") from e
    finally:
        close_log(log)

    return class_schema

def parse_type(log, type_str):
    """
    Parses a type string to a SQLAlchemy column type.

    Parameters:
    type_str (str): The string representing a SQLAlchemy data type.

    Returns:
    Type: SQLAlchemy column type.

    Raises:
    Exception: If the type string cannot be parsed.
    """
    log.info("Parsing type string: %s", type_str)

    try:
        if '(' in type_str:
            base_type, params = type_str.split('(')
            param = int(params.rstrip(')'))
            return getattr(sqlalchemy, base_type)(param)
        return getattr(sqlalchemy, type_str)
    except Exception as e:
        log.error("Error parsing type string: %s\n%s", e, traceback.format_exc())
        close_log(log)
        raise

def create_class_schema(json_data):
    """
    Creates a class schema from JSON data.

    Parameters:
    json_data (dict): JSON data containing the schema information.

    Returns:
    dict: A dictionary of attributes for the class.

    Raises:
    Exception: If the class schema cannot be created.
    """
    log = open_log(json_data['table_name'])
    log.info("Creating class schema for table: %s", json_data['table_name'])

    try:
        attrs = {'__tablename__': json_data['table_name'],
                 '__table_args__': {'extend_existing': True},
                 '__unique_identifier__': json_data['identifier']}
        attrs['id'] = Column(Integer, primary_key=True, autoincrement=True)

        for col in json_data['columns']:
            column_type = parse_type(log, col['type'])
            attrs[col['name']] = Column(column_type)

        # Add standard columns at the end
        attrs['version_number'] = Column(Integer, default=1)
        attrs['effective_date'] = Column(Date, default=datetime.now)
        attrs['expiry_date'] = Column(Date, default=None)

        log.info("Class schema created successfully for table: %s", json_data['table_name'])
    except Exception as e:
        log.error("Error creating class schema: %s\n%s", e, traceback.format_exc())
        raise
    finally:
        close_log(log)

    return attrs

def create_class(attrs):
    """
    Creates a dynamic class based on the provided attributes.

    Parameters:
    attrs (dict): Attributes to be included in the class.

    Returns:
    Class: A dynamically created class.

    Raises:
    Exception: If the class cannot be created.
    """
    log = open_log(attrs['__tablename__'])

    try:
        log.info("Creating dynamic class for %s table", attrs['__tablename__'])
        Base = declarative_base()

        # Add dynamic __repr__ method
        #column_names = [key for key in attrs if not key.startswith('__')]
        #repr_string = create_repr_string(attrs['__tablename__'], column_names)
        #attrs['__repr__'] = lambda self: repr_string.format(self=self)

        # Assign functions as class methods
        #attrs['create_session'] = classmethod(create_session)
        #attrs['create_table'] = classmethod(create_table)
        #attrs['add_data'] = classmethod(add_data)

        return type(attrs['__tablename__'], (Base,), attrs)
    except Exception as e:
        log.error("Error creating class: %s\n%s", e, traceback.format_exc())
        raise
    finally:
        close_log(log)

def open_class(class_name):
    """
    Opens a class by loading its JSON, creating its schema, and then the class itself.

    Parameters:
    class_name (str): The name of the class to be opened.

    Returns:
    Class: The dynamically created class.

    Raises:
    JsonLoaderError: If there is an issue with the JSON loading.
    """
    log = open_log(class_name)
    log.info("Opening %s class logger", class_name)

    try:
        json_data = load_class_json(class_name)
        class_attrs = create_class_schema(json_data)
        table_class = create_class(class_attrs)

        log.info("Class successfully opened: %s", class_name)
        return table_class
    
    except JsonLoaderError as e:
        log.error("JsonLoaderError encountered: %s", e)
        raise
    finally:
        log.info("Closing %s class logger", class_name)
        close_log(log)

# Example usage
regions = open_class("REGIONS")
regions
    
