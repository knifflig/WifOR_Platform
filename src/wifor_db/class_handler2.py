# Standard library imports
import os
import json
from datetime import datetime, timedelta
import logging
import traceback
from collections import defaultdict

# Third-party imports
import sqlalchemy
from sqlalchemy import Column, Integer, Date, create_engine, inspect, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# Local application imports
from wifor_db import _env_cache, open_log, close_log

class TABLE_CONNECTOR:
    def __init__(self):
        self.log = open_log("CONNECTOR_LOG")
        self.Base = declarative_base()
        self.engine = None
        self.session = None

    def __enter__(self):
        self.log.info("OPEN CONNECTOR LOG")
        self.engine = self.create_engine_from_env()
        self.session = self.create_session(self.engine)
        self.register_before_flush_event(self.session)
        self.log.info("session created")
        
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.session:
            self.session.close()
            self.log.info("session closed")
        if self.log:
            self.log.info("CLOSE CONNECTOR LOG")
            close_log(self.log)

    @staticmethod
    def create_engine_from_env():
        current_db = _env_cache['CURRENT_DB']
        if current_db == 'sqlite':
            db_url = _env_cache['SQLITE_DB_PATH']
        elif current_db == 'mysql':
            db_url = f"mysql+pymysql://{_env_cache['MYSQL_DB_USER']}:{_env_cache['MYSQL_DB_PASSWORD']}@{_env_cache['MYSQL_DB_HOST']}/{_env_cache['MYSQL_DB_NAME']}"
        elif current_db == 'postgresql':
            db_url = f"postgresql://{_env_cache['POSTGRES_DB_USER']}:{_env_cache['POSTGRES_DB_PASSWORD']}@{_env_cache['POSTGRES_DB_HOST']}:{_env_cache['POSTGRES_DB_PORT']}/{_env_cache['POSTGRES_DB_NAME']}"
        else:
            raise ValueError(f"Unsupported database type: {current_db}")
        return create_engine(db_url)
    
    @staticmethod
    def create_session(engine):
        if not engine:
            raise ValueError("Engine not initialized")
        Session = sessionmaker(bind=engine)
        return Session()

##############################################################################################################
    @staticmethod
    def load_class_json(self, class_name):
        json_path = os.path.join(_env_cache['CLASS_DIR'], f"{class_name}.json")
        with open(json_path, 'r', encoding="utf-8") as file:
            self.log.info("""open json in path:
                           %s""", json_path)
            return json.load(file)

    @staticmethod
    def parse_type(self, type_str):
        if '(' in type_str:
            base_type, params = type_str.split('(')
            param = int(params.rstrip(')'))
            return getattr(sqlalchemy, base_type)(param)
        return getattr(sqlalchemy, type_str)
    
    @staticmethod
    def create_repr_string(self, name, columns):
        self.log.info("""create repr string from
                      name: %s
                      columns: %s""", name, columns)
        repr_parts = [f"{column['name']}='{{self.{column['name']}}}'" if 'String' in column['type'] else f"{column['name']}={{self.{column['name']}}}" for column in columns]
        standard_parts = ["version_number={self.version_number}", "effective_date='{self.effective_date}'", "expiry_date='{self.expiry_date}'"]
        return f"<{name}(" + ', '.join(repr_parts + standard_parts) + ")>"

    @staticmethod
    def create_class_schema(self, json_data):
        attrs = {'__tablename__': json_data['table_name'],
                 '__table_args__': {'extend_existing': True},
                 '__unique_identifier__': json_data['identifier'],
                 '__column_names__': [column['name'] for column in json_data["columns"]],
                 'id': Column(Integer, primary_key=True, autoincrement=True)}
        
        # Add dynamic __repr__ method
        repr_string = self.create_repr_string(self, attrs["__tablename__"], json_data['columns'])
        attrs['__repr__'] = lambda self: repr_string.format(self=self)

        for col in json_data['columns']:
            column_type = self.parse_type(self, col['type'])
            attrs[col['name']] = Column(column_type)

        attrs['version_number'] = Column(Integer, default=1)
        attrs['effective_date'] = Column(Date, default=datetime.now)
        attrs['expiry_date'] = Column(Date, default=None)

        return attrs
    
########################################################################################################################
    
    def update_entries(self, previous_entry, new_entry):
        """Update the expiry_date of the previous entry."""
        self.log.info("Update previous entry: %s and new entry: %s", previous_entry, new_entry)
        previous_entry.expiry_date = datetime.now() - timedelta(days=1)
        new_entry.version_number = previous_entry.version_number + 1

    def bulk_check_existing_entries(self, session, cls, new_entries):
        # Extract unique identifiers for all new entries
        unique_ids = [getattr(instance, cls.__unique_identifier__) for instance in new_entries]

        # Query the database for these identifiers
        existing_entries = session.query(cls).filter(
            getattr(cls, cls.__unique_identifier__).in_(unique_ids)
        ).all()

        # Convert existing entries to a set for easy lookup
        existing_set = set(getattr(entry, cls.__unique_identifier__) for entry in existing_entries)

        # Return a set of instances that are duplicates
        return {instance for instance in new_entries if getattr(instance, cls.__unique_identifier__) in existing_set}
    
    def bulk_check_previous_versions(self, session, cls, new_entries):
        # Extract unique identifiers for all new entries
        unique_ids = [getattr(instance, cls.__unique_identifier__) for instance in new_entries]

        # Query the database for entries with expiry_date None
        previous_versions = session.query(cls).filter(
            getattr(cls, cls.__unique_identifier__).in_(unique_ids),
            cls.expiry_date.is_(None)
        ).all()

        # Map unique identifiers to previous version instances
        previous_versions_map = {getattr(entry, cls.__unique_identifier__): entry for entry in previous_versions}

        return previous_versions_map

    def register_before_flush_event(self, session):
        @event.listens_for(session, "before_flush")
        def before_flush(session, flush_context, instances):
            self.log.info("Before flush event triggered")

            new_entries_by_class = defaultdict(list)
            for instance in session.new:
                new_entries_by_class[type(instance)].append(instance)

            for cls, new_entries in new_entries_by_class.items():
                existing_entries = self.bulk_check_existing_entries(session, cls, new_entries)
                previous_versions = self.bulk_check_previous_versions(session, cls, new_entries)

                for instance in new_entries:
                    if instance in existing_entries:
                        self.log.info(f"Duplicate entry found for {instance}. It will not be added to the database.")
                        session.expunge(instance)
                    elif getattr(instance, cls.__unique_identifier__) in previous_versions:
                        previous_entry = previous_versions[getattr(instance, cls.__unique_identifier__)]
                        self.log.info(f"Previous version exists for {instance}. Updating entries.")
                        self.update_entries(previous_entry, instance)

    def process_new_entries_for_class(self, session, cls, new_entries):
        # Bulk check for existing entries and previous versions
        existing_entries = self.bulk_check_existing_entries(session, cls, new_entries)
        previous_versions = self.bulk_check_previous_versions(session, cls, new_entries)

        for instance in new_entries:
            if instance in existing_entries:
                session.expunge(instance)
            elif instance in previous_versions:
                self.update_entries(previous_versions[instance], instance)

    def add_class_methods(self, cls):
        session = self.session

        @classmethod
        def init_table(cls):
            engine = session.get_bind()
            if not inspect(engine).has_table(cls.__tablename__):
                cls.metadata.create_all(engine)

        cls.init_table = init_table

        @classmethod
        def add_data(cls, data):
            filtered_data = data[cls.__column_names__]
            for _, row in filtered_data.iterrows():
                instance = cls(**row.to_dict())
                session.add(instance)
            session.commit()

        cls.add_data = add_data

    def open_table(self, class_name):
        json_data = self.load_class_json(self, class_name)
        class_attrs = self.create_class_schema(self, json_data)
        dynamic_class = type(json_data['table_name'], (self.Base,), class_attrs)

        self.add_class_methods(dynamic_class)

        return dynamic_class