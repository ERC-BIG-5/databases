import os
from datetime import datetime
from pathlib import Path

import pytest
from sqlalchemy import inspect
from sqlalchemy.orm import Session

from big5_databases.databases.db_mgmt import DatabaseManager
from big5_databases.databases.db_models import DBPost
from big5_databases.databases.external import DBConfig, SQliteConnection


def setup_function(function):
    """Setup test database file."""
    if os.path.exists("test.sqlite"):
        os.remove("test.sqlite")

def teardown_function(function):
    """Teardown test database file."""
    if os.path.exists("test.sqlite"):
        os.remove("test.sqlite")

@pytest.fixture
def test_sqlite_db_config() -> DBConfig:
    return DBConfig(db_connection=SQliteConnection(db_path=Path("test.sqlite")))

def test_create_engine(test_sqlite_db_config):
    """Test that the _create_engine method creates an engine."""
    #config = DatabaseConfig("sqlite",
    db_manager = DatabaseManager(test_sqlite_db_config)

    engine = db_manager._create_engine()

    assert engine is not None
    assert engine.url.database == "test.sqlite"

def test_init_database(test_sqlite_db_config):
    """Test that init_database creates the tables."""
    db_manager = DatabaseManager(test_sqlite_db_config)

    db_manager.init_database()

    inspector = inspect(db_manager.engine)
    # todo test more
    assert "post" in inspector.get_table_names()

def test_get_session(test_sqlite_db_config):
    """Test that get_session provides a working session."""
    db_manager = DatabaseManager(test_sqlite_db_config)
    db_manager.init_database()

    with db_manager.get_session() as session:
        assert session is not None
        assert isinstance(session, Session)

def test_write_and_read_objects(test_sqlite_db_config):
    """Test writing to and reading from the database."""
    db_manager = DatabaseManager(test_sqlite_db_config)
    db_manager.init_database()

    with db_manager.get_session() as session:
        new_entry = DBPost(platform="youtube",platform_id="djksajksjak", date_created=datetime.now())
        session.add(new_entry)


    with db_manager.get_session() as session:
        result = session.query(DBPost).first()
        assert result is not None
        #assert result.name == "Test Name"
