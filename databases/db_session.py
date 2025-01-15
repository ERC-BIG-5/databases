"""
@dedprecated use db_mgmt.py
"""
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database, drop_database

from databases.db_models import Base
from databases.db_settings import PostgresCredentials
from tools.project_logging import get_logger

logger = get_logger(__file__)

def create_postgres_db(db_name: str, drop_existing: bool = False) -> None:
    """
    Create the database and tables based on the defined models.

    """
    conn_str = PostgresCredentials(DB_NAME=db_name).connection_str
    create = False
    if database_exists(conn_str):
        if drop_existing:
            answer = input(f"Database {db_name} already exists. Do you want to drop it? (y/n)")
            if answer == "y":
                drop_database(conn_str)
                create = True
            else:
                print("Aborting")
                return
    else:
        create = True
    if create:
        create_database(conn_str)

    engine = create_engine(conn_str)
    Base.metadata.create_all(engine)


