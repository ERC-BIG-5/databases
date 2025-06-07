import os
from pathlib import Path
from typing import Optional

from alembic import command
from alembic.config import Config
from sqlalchemy import inspect, create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import declarative_base

from databases.db_models import Base
from databases.external import SQliteConnection
from tools.project_logging import get_logger

logger = get_logger(__file__)

# Configure Alembic programmatically
def run_migrations(connection_str: str, base, alembic_dir: Optional[str]) -> None:

    # Configure Alembic programmatically
    alembic_cfg = Config()
    alembic_cfg.set_main_option("script_location", alembic_dir)
    alembic_cfg.set_main_option("sqlalchemy.url", connection_str)
    alembic_cfg.config_file_name =  "alembic.ini"
    alembic_cfg.declarative_base = base

    # Generate migration script
    logger.info(f"Generating migration script for {connection_str}")
    command.revision(alembic_cfg, autogenerate=True, message="Auto-generated migration")

    # Apply migrations
    logger.info(f"Applying migrations to {connection_str}")
    command.upgrade(alembic_cfg, "head")

def update_db_schema(engine, base):
    """Update the database schema based on ORM models."""
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()

    # Compare the current schema with the ORM models
    for table in base.metadata.tables.values():
        if table.name not in existing_tables:
            logger.info(f"Table {table.name} does not exist. Creating table.")
            table.create(engine)
        else:
            logger.info(f"Table {table.name} exists. Checking columns.")
            existing_columns = {col['name'] for col in inspector.get_columns(table.name)}

            for column in table.columns:
                if column.name not in existing_columns:
                    logger.info(f"Column {column.name} does not exist in {table.name}. Adding column.")
                    try:
                        with engine.connect() as conn:
                            conn.execute(text(f"ALTER TABLE {table.name} ADD COLUMN {column.name} {str(column.type)}"))

                    except OperationalError as e:
                        logger.error(f"Failed to add column {column.name} to {table.name}: {e}")

                elif column.name in existing_columns:
                    # Handle column constraints (if necessary)
                    pass



if __name__ == "__main__":
    # Choose the mode based on your use case
    con = SQliteConnection(db_path=Path("/home/rsoleyma/projects/platforms-clients/data/twitter.sqlite"))
    #run_migrations(con.connection_str, Base, "alembic")
    engine = create_engine(con.connection_str)
    update_db_schema(engine, Base)