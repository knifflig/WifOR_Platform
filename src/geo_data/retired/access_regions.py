"""
This module, `geo_data`, provides functionality for interacting with a database using
SQLAlchemy ORM. It primarily consists of the `Regions` class, which defines a
representation of the 'regions' table in the database.

Classes:
    Regions: An ORM class mapped to the 'regions' table in the database. It includes
    columns for region identifiers, codes, names, types, and geographic information.
    It also has methods for database initialization and session management.

The module supports creating a new database, handling sessions, and performing basic
ORM operations like adding and querying entries. The `Regions` class includes
methods for initializing the database (`init_db`) and handling before_flush events
to manage record versioning and expiry dates.

Example:
    from geo_data import Regions
    Session = Regions.init_db('path_to_database.db')
    with Session() as session:
        region = Regions(NUTS_ID='ID123', LEVL_CODE=2, ...)
        session.add(region)
        session.commit()

Note:
    Ensure the database URI is correctly formatted and the file path is accurate when
    initializing the database.
"""

import os
#import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, Date, inspect, event
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError
from wifor_db import setup_logger

# Load environment variables
load_dotenv()

# set up logger
script_dir = os.path.dirname(os.path.abspath(__file__))
logger = setup_logger("access_regions", script_dir)
if logger:
    logger.info("Logging setup complete")
else:
    print("Logger setup failed.")

# Set up Base instance
Base = declarative_base()

class Regions(Base):
    """
    ORM class representing the 'regions' table in the database.

    Attributes:
        id (Integer): The primary key, auto-incrementing.
        NUTS_ID (String): Identifier for NUTS region.
        LEVL_CODE (Integer): Level code of the region.
        CNTR_CODE (String): Country code.
        NAME_LATN (String): Latin name of the region.
        NUTS_NAME (String): NUTS name of the region.
        MOUNT_TYPE (String): Mountain type classification.
        URBN_TYPE (String): Urban type classification.
        COAST_TYPE (String): Coastal type classification.
        FID (String): Feature identifier.
        geometry (String): Geometry information.
        version_number (Integer): Version number of the record.
        effective_date (Date): Date when the record becomes effective.
        expiry_date (Date): Date when the record expires.
    """

    __tablename__ = 'regions'

    # Column definitions
    id = Column(Integer, primary_key=True, autoincrement=True)
    NUTS_ID = Column(String(255))
    LEVL_CODE = Column(Integer)
    CNTR_CODE = Column(String(255))
    NAME_LATN = Column(String(255))
    NUTS_NAME = Column(String(255))
    MOUNT_TYPE = Column(String(255))
    URBN_TYPE = Column(String(255))
    COAST_TYPE = Column(String(255))
    FID = Column(String(255))
    geometry = Column(String)
    version_number = Column(Integer, default=1)
    effective_date = Column(Date, default=datetime.now())
    expiry_date = Column(Date, default=None)

    def __repr__(self):
        """
        Representation of the Regions instance.
        
        Returns:
            str: A string representation of the instance.
        """
        # pylint: disable=line-too-long
        return f"<Regions(NUTS_ID='{self.NUTS_ID}', LEVL_CODE={self.LEVL_CODE}, CNTR_CODE='{self.CNTR_CODE}', NAME_LATN='{self.NAME_LATN}', NUTS_NAME='{self.NUTS_NAME}', MOUNT_TYPE='{self.MOUNT_TYPE}', URBN_TYPE='{self.URBN_TYPE}', COAST_TYPE='{self.COAST_TYPE}', FID='{self.FID}', geometry='{self.geometry}', version_number={self.version_number}, effective_date='{self.effective_date}', expiry_date='{self.expiry_date}')>"
        # pylint: enable=line-too-long

    @classmethod
    def before_flush(cls, session, flush_context, instances):
        # pylint: disable=unused-argument
        """
        Before flush event handler to update expiry dates and version numbers of Region entries.

        This method is triggered before a session is flushed.
        It checks new entries against existing ones.
        If a new entry is identical to an existing entry,
        the new one is removed to avoid duplication.
        Otherwise, it updates the expiry date of existing records to one day before now
        and increments the version number for the new entries.

        Args:
            session (Session): The session being flushed.
            flush_context (object): The context of the flush.
            instances (list): Instances involved in the flush.

        Raises:
            SQLAlchemyError: If any database operation fails.
        """
        try:
            for instance in session.new:
                if isinstance(instance, Regions):
                    try:
                        same_entry = session.query(Regions).filter_by(
                            NUTS_ID=instance.NUTS_ID,
                            LEVL_CODE=instance.LEVL_CODE,
                            CNTR_CODE=instance.CNTR_CODE,
                            NAME_LATN=instance.NAME_LATN,
                            NUTS_NAME=instance.NUTS_NAME,
                            MOUNT_TYPE=instance.MOUNT_TYPE,
                            URBN_TYPE=instance.URBN_TYPE,
                            COAST_TYPE=instance.COAST_TYPE,
                            FID=instance.FID
                        ).first()

                        if same_entry:
                            # If entries are identical, remove the new instance
                            session.expunge(instance)
                            # pylint: disable=line-too-long
                            logger.info("Duplicate entry found for NUTS_ID %s. Instance removed.", instance.NUTS_ID)
                            # pylint: enable=line-too-long

                        else:
                            previous_entry = session.query(Regions).filter_by(
                                    NUTS_ID=instance.NUTS_ID, expiry_date=None).first()

                            if previous_entry:
                                # Update the existing entry expiry date
                                new_effective_date = datetime.now() - timedelta(days=1)
                                previous_entry.expiry_date = new_effective_date

                                #increment version number of new entry
                                instance.version_number = previous_entry.version_number + 1

                                # pylint: disable=line-too-long
                                logger.info("Updating existing entry and setting version number for new entry with NUTS_ID %s.", instance.NUTS_ID)
                                # pylint: enable=line-too-long

                    except SQLAlchemyError as e:
                        # pylint: disable=line-too-long
                        logger.exception("Error processing instance with NUTS_ID %s: %s", instance.NUTS_ID, e)
                        # pylint: enable=line-too-long

        # pylint: disable=broad-except
        except Exception as e:
            logger.exception("Unexpected error in before_flush: %s", e)
        # pylint: enable=broad-except, unused-argument

    @staticmethod
    def init_db():
        """
        Initialize the database connection using environment variables.

        This method reads the database configuration from environment variables,
        creates a SQLAlchemy engine based on these configurations, and establishes
        a new session. It supports switching between SQLite and MySQL databases.

        Returns:
            sessionmaker: A configured SQLAlchemy sessionmaker object if successful,
                          None otherwise.
        """
        try:
            # Load environment variables
            load_dotenv()

            current_db = os.environ.get('CURRENT_DB')

            # Check which database to connect to
            if current_db == 'sqlite':
                db_path = os.environ.get('SQLITE_DB_PATH')
                if not db_path:
                    raise ValueError("SQLite database path is not set in .env file.")
                engine = create_engine(db_path, echo=False)

            elif current_db == 'mysql':
                db_user = os.environ.get('MYSQL_DB_USER')
                db_password = os.environ.get('MYSQL_DB_PASSWORD')
                db_host = os.environ.get('MYSQL_DB_HOST')
                db_name = os.environ.get('MYSQL_DB_NAME')

                if not all([db_user, db_password, db_host, db_name]):
                    raise ValueError("MySQL credentials are not set properly in .env file.")

                db_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}"
                engine = create_engine(db_url, echo=False)

            else:
                raise ValueError("""
                                 Database type is not defined or not supported.
                                 Please set 'sqlite' or 'mysql' in the .env file.
                                 """)

            # Proceed with engine setup
            if not inspect(engine).has_table(Regions.__tablename__):
                Base.metadata.create_all(engine)
                print("The table has been created in the database.")

            session = sessionmaker(bind=engine)
            event.listen(session, 'before_flush', Regions.before_flush)
            return session

        except SQLAlchemyError as e:
            print(f"SQLAlchemy Error: {e}")
            return None
        except ValueError as e:
            print(f"Configuration Error: {e}")
            return None
        # pylint: disable=broad-except
        except Exception as e:
            print(f"Unexpected Error: {e}")
            return None
        # pylint: enable=broad-except
        