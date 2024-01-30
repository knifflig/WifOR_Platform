"""
The class_handler module provides a set of utilities for dynamic handling of SQLAlchemy model classes. 

This module allows for dynamic creation of SQLAlchemy model classes based on JSON schemas, managing database sessions, and handling data operations with the database. It includes functionality to set up logging for each class, create and manage SQLAlchemy sessions, and dynamically create model classes with custom behaviors. The module is designed to work with both SQLite and MySQL databases, with database configurations being read from environment variables.

Main Functionalities:
- setup_logger: Sets up a logger for a given class, writing logs to a file named after the class in a 'log_files' directory.
- close_logger: Closes the logger by removing all its handlers.
- create_repr_string: Generates a string representation for a SQLAlchemy model class based on its columns.
- before_flush: A custom 'before_flush' event handler for SQLAlchemy models to handle duplicate entries and manage record versioning.
- create_engine_from_env: Creates a SQLAlchemy engine based on environment variables, supporting SQLite and MySQL databases.
- create_session: Creates a SQLAlchemy session using an engine obtained from 'create_engine_from_env'.
- create_table: Initializes the database and creates a table if it does not exist, returning a session for querying.
- add_data: Adds data from a pandas DataFrame to the database table associated with a given class.
- create_class: Dynamically creates a SQLAlchemy model class based on a provided JSON schema, including standard and schema-defined columns, a dynamic '__repr__' method, class methods for database operations, and a custom 'before_flush' method.

This module is essential for applications that require dynamic database model handling and logging, particularly in cases where the database schema is subject to change or needs to be inferred from external configurations.

Note:
- The module assumes the presence of a .env file for environment variable management.
- JSON schemas used for class creation must follow a specific format detailing the table name, columns, and a unique identifier.
- Logging is an integral part of this module, with comprehensive logs generated for each significant action and error.
"""
# pylint: disable=logging-fstring-interpolation, line-too-long

import json
import os
from datetime import datetime, timedelta
import logging
import traceback
from typing import Any, List, Dict
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, Date, inspect, types, ForeignKey
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from sqlalchemy.exc import SQLAlchemyError

Base = declarative_base()

def setup_logger(class_name):
    """
    Sets up a logger that writes to a file named after the class. 
    The log file is stored in a 'log_files' directory relative to the script's location.

    Args:
        class_name (str): name of the class for which the logger is being set up.

    Returns:
        logging.Logger: Configured logger object. Returns None if an error occurs during setup.
    """
    # Load environment variables
    load_dotenv()

    # set up logger for the script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    try:
        log_dir = os.path.join(script_dir, "log_files")
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        log_file_path = os.path.join(log_dir, f"{class_name}.log")

        logger = logging.getLogger(class_name)
        logger.setLevel(logging.INFO)

        # Check if the logger already has handlers to avoid duplicate logs
        if not logger.handlers:
            file_handler = logging.FileHandler(log_file_path)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            #logger.info(f"Logger for {class_name} setup complete with file handler.")

        return logger

    except Exception as e:
        print(f"Error setting up logger for {class_name}: {e}")
        return None
    
def close_logger(logger):
    """
    Closes the logger by removing and closing all its handlers and logs a message before closing.

    Args:
        logger (logging.Logger): The logger to be closed.
    """
    # Log closing message
    logger.info("Logger closed")

    handlers = logger.handlers[:]
    for handler in handlers:
        handler.close()
        logger.removeHandler(handler)

def create_repr_string(name: str, columns: List[Dict[str, str]], logger):
    """
    Create a string representation for a SQLAlchemy model class.

    Args:
        name (str): The name of the model class.
        columns (List[Dict[str, str]]): A list of dictionaries, each representing a column in the table.
                                        Each dictionary should have keys like 'name' and 'type'.

    Returns:
        Optional[str]: A string that can be used as the __repr__ method for a SQLAlchemy model class.
                       Returns None if an error occurs.

    Raises:
        logs an error message if an exception occurs during string creation.
    """
    try:
        repr_parts = [f"{column['name']}='{{self.{column['name']}}}'" if 'String' in column['type'] else f"{column['name']}={{self.{column['name']}}}" for column in columns]
        standard_parts = ["version_number={self.version_number}", "effective_date='{self.effective_date}'", "expiry_date='{self.expiry_date}'"]
        return f"<{name}(" + ', '.join(repr_parts + standard_parts) + ")>"

    except Exception as e:
        logger.error(f"Error creating representation string: {e}\n{traceback.format_exc()}")
        return None

def before_flush(cls, session: Session, flush_context: Any, instances: Any, columns: List[Dict[str, str]], unique_identifier: str, logger):
    """
    Custom 'before_flush' event handler for SQLAlchemy models.

    This function is designed to be used as a class method in SQLAlchemy models. It checks for
    duplicate entries and manages the versioning of records.

    Args:
        cls (Type[Any]): The class on which the event was invoked.
        session (Session): The session which is flushing.
        flush_context (Any): The context for the flush.
        instances (Any): The set of instances participating in the flush.
        columns (List[Dict[str, str]]): A list of dictionaries representing the columns of the table.
        unique_identifier (str): The field name used as a unique identifier for the entries.

    Raises:
        logs an error message if an exception occurs during the flush process.
    """
    logger.info("Starting before_flush process.")
    try:
        for instance in session.new:
            if isinstance(instance, cls):
                filter_args = {col['name']: getattr(instance, col['name']) for col in columns}
                same_entry = session.query(cls).filter_by(**filter_args).first()

                if same_entry:
                    # If entries are identical, remove the new instance
                    session.expunge(instance)
                else:
                    # Check for previous entry with the same unique identifier and no expiry date
                    previous_entry = session.query(cls).filter_by(**{unique_identifier: getattr(instance, unique_identifier), 'expiry_date': None}).first()

                    if previous_entry:
                        # Update the existing entry expiry date
                        new_effective_date = datetime.now() - timedelta(days=1)
                        previous_entry.expiry_date = new_effective_date

                        # Increment version number of new entry
                        instance.version_number = previous_entry.version_number + 1

    except Exception as e:
        logger.error(f"Error in before_flush: {e}\n{traceback.format_exc()}")

def create_engine_from_env(logger):
    """
    Creates a SQLAlchemy engine based on environment variables.

    This function supports creating engines for both SQLite and MySQL databases.
    The database type and credentials are read from environment variables.

    Returns:
        sqlalchemy.engine.Engine: SQLAlchemy engine if successful, None otherwise.

    Raises:
        Logs an error message using the configured logger if an exception occurs during engine creation.
    """
    logger.info("Starting to create database engine from environment.")
    try:
        load_dotenv()
        current_db = os.environ.get('CURRENT_DB')

        if current_db == 'sqlite':
            db_path = os.environ.get('SQLITE_DB_PATH')
            if not db_path:
                raise ValueError("SQLite database path is not set in .env file.")
            db_url = f"{db_path}"
            logger.info(f"Connecting to SQLite database at: {db_url}")
            return create_engine(db_url, echo=False)

        elif current_db == 'mysql':
            db_user = os.environ.get('MYSQL_DB_USER')
            db_password = os.environ.get('MYSQL_DB_PASSWORD')
            db_host = os.environ.get('MYSQL_DB_HOST')
            db_name = os.environ.get('MYSQL_DB_NAME')

            if not all([db_user, db_password, db_host, db_name]):
                raise ValueError("MySQL credentials are not set properly in .env file.")

            db_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}"
            return create_engine(db_url, echo=False)

        else:
            raise ValueError("Database type is not defined or not supported.")

    except ValueError as e:
        logger.error(f"Configuration Error: {e}\n{traceback.format_exc()}")
    except SQLAlchemyError as e:
        logger.error(f"SQLAlchemy Engine Creation Error: {e}\n{traceback.format_exc()}")
    except Exception as e:
        logger.error(f"Unexpected Error: {e}\n{traceback.format_exc()}")
    return None

def create_session(logger):
    """
    Create and return a SQLAlchemy session.

    This function creates a SQLAlchemy session using an engine obtained from `create_engine_from_env`.

    Returns:
        sqlalchemy.orm.session.Session: A SQLAlchemy session if the engine is successfully created and connected, None otherwise.

    Raises:
        Logs an error message using the configured logger if any exception occurs during session creation.
    """
    logger.info("Starting session creation.")
    engine = create_engine_from_env(logger)
    try:
        if engine is None:
            raise ValueError("Engine cannot be None.")
        Session = sessionmaker(bind=engine)
        return Session()
    except SQLAlchemyError as e:
        logger.error(f"SQLAlchemy Error: {e}\n{traceback.format_exc()}")
    except ValueError as e:
        logger.error(f"Configuration Error: {e}\n{traceback.format_exc()}")
    except Exception as e:
        logger.error(f"Unexpected Error: {e}\n{traceback.format_exc()}")
    return None

def create_table(cls):
    """
    Initialize the database, create the table if it does not exist, and return a session for querying.

    :param cls: The class for which the table is to be created.
    :return: A session object or None in case of failure.
    """
    # Pylint may not recognize dynamic attributes like 'logger', so we disable the no-member warning
    # pylint: disable=no-member

    session = None  # Initialize the session variable

    try:
        # Setup logger
        logger = setup_logger(cls.__tablename__)
        if logger is None:
            raise ValueError("Logger setup failed.")
        logger.info(f"Logger is set up for class {cls.__tablename__}")

        # Create a session
        session = create_session(logger)
        if session is None:
            raise ValueError("Failed to create a session.")
        logger.info("session created")

        engine = session.get_bind()

        # Check if the table exists and create it if not
        if not inspect(engine).has_table(cls.__tablename__):
            cls.metadata.create_all(engine)
            logger.info(f"The table '{cls.__tablename__}' has been created in the database.")
        else:
            logger.info(f"The table '{cls.__tablename__}' already exists.")

        return session

    except SQLAlchemyError as e:
        if logger:
            logger.error(f"SQLAlchemy Error: {e}\n{traceback.format_exc()}")
        return None
    except ValueError as e:
        if logger:
            logger.error(f"Configuration Error: {e}\n{traceback.format_exc()}")
        return None
    except Exception as e:
        if logger:
            logger.error(f"Unexpected Error: {e}\n{traceback.format_exc()}")
        return None

    finally:
        # Close the session if it was successfully created
        if session:
            session.close()
            logger.info("Session closed.")

        if logger:
            close_logger(logger)

def add_data(cls, data, column_names):
    """
    Add data from a pandas or geopandas DataFrame to the database table.
    """
    # Setup logger
    logger = setup_logger(cls.__tablename__)
    if logger is None:
        raise ValueError("Logger setup failed.")
    logger.info(f"Logger is set up for class {cls.__tablename__}")

    if not isinstance(data, pd.DataFrame):
        raise ValueError("Data must be a pandas or geopandas DataFrame")

    filtered_data = data[column_names]
    session = create_session(logger)

    try:
        for _, row in filtered_data.iterrows():
            instance = cls(**row.to_dict())
            session.add(instance)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e

    finally:
        # Close the session if it was successfully created
        if session:
            session.close()
            logger.info("Session closed.")

        if logger:
            close_logger(logger)

def create_class(json_path: str):
    """
    Dynamically creates a SQLAlchemy model class based on a JSON schema.

    Args:
        json_path (str): The file path to the JSON schema.

    Returns:
        Optional[Type[Base]]: A dynamically created SQLAlchemy model class derived from Base, or None if an error occurs.

    Raises:
        ValueError: If the schema loading fails or necessary attributes are missing.
        AttributeError: If there are issues with setting attributes or methods on the class.
        Exception: For any other unexpected errors.

    The created class includes standard and schema-defined columns, a dynamic __repr__ method, 
    class methods for database operations, and a custom before_flush method.
    """
    if not os.path.exists(json_path):
        raise FileNotFoundError(f"The file {json_path} does not exist.")

    try:
        with open(json_path, 'r', encoding="utf-8") as file:
            schema = json.load(file)

    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(f"Error decoding JSON from {json_path}: {e}")

    except Exception as e:
        raise Exception(f"An error occurred while reading {json_path}: {e}")

    if schema is None:
        raise ValueError(f"Schema loading failed for file {json_path}.")

    try:
         # Set up logger for the class
        logger = setup_logger(schema['table_name'])
        logger.info(f"Logger is set up for class {schema['table_name']}")

        column_names = [column['name'] for column in schema['columns']]
        logger.info(f"Columns extracted: {column_names}")

        # Initialize attrs with table name, columns, and unique identifier
        attrs = {
            '__tablename__': schema['table_name']
            , 'columns': column_names
            , 'unique_identifier': schema['identifier']
        }
        logger.info(f"Initial attributes set for the class: {attrs}")

        # Add standard columns at the beginning
        attrs['id'] = Column(Integer, primary_key=True, autoincrement=True)

        # Add columns from JSON schema
        for column in schema['columns']:
            logger.info(f"Processing column: {column}")
            column_type = getattr(types, column['type'].split('(')[0])
            if '(' in column['type']:
                size = int(column['type'].split('(')[1].replace(')', ''))
                column_type = column_type(size)
            attrs[column['name']] = Column(column_type)

        # Add standard columns at the end
        attrs['version_number'] = Column(Integer, default=1)
        attrs['effective_date'] = Column(Date, default=datetime.now)
        attrs['expiry_date'] = Column(Date, default=None)
        logger.info("Standard columns added.")

        # Add dynamic __repr__ method
        repr_string = create_repr_string(schema['table_name'], schema['columns'], logger)
        attrs['__repr__'] = lambda self: repr_string.format(self=self)
        logger.info("Dynamic __repr__ method added to class")

        # Add dynamic before_flush class method
        attrs['before_flush'] = classmethod(lambda cls, session, flush_context, instances: before_flush(cls, session, flush_context, instances, schema['columns'], attrs['unique_identifier'], logger))
        logger.info("Dynamic before_flush method added to class")

        # Assign functions as class methods
        attrs['create_session'] = classmethod(lambda cls: create_session(logger))
        attrs['create_table'] = classmethod(create_table)
        attrs['add_data'] = classmethod(add_data)
        logger.info("Class methods assigned.")

        logger.info(f"Class {schema['table_name']} successfully created.")
        return type(schema['table_name'], (Base,), attrs)

    except ValueError as e:
        logger.error(f"ValueError in create_class: {e}\n{traceback.format_exc()}")
    except AttributeError as e:
        logger.error(f"AttributeError in create_class: {e}\n{traceback.format_exc()}")
    except Exception as e:
        logger.error(f"Unexpected error in create_class: {e}\n{traceback.format_exc()}")
        raise

    finally:
        close_logger(logger)
