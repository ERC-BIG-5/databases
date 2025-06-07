"""
Trying to resort by date_created... but does not work...
"""

import logging
from datetime import datetime
from pathlib import Path

from sqlalchemy import Table, MetaData, text

from big5_databases.databases.db_mgmt import DatabaseManager


def check_sorted(db_path: Path, table: str = "post") -> bool:
    db = DatabaseManager.sqlite_db_from_path(db_path)

    #current_dt: datetime = None
    with db.get_session() as session:
        current_dt = datetime.fromisoformat(list(session.execute(text(f"select date_created from {table} limit 1")).scalars())[0])

    for post_time in session.execute(text(f"select date_created from {table}")).scalars():
        post_time_ = datetime.fromisoformat(post_time)
        if post_time_ >= current_dt:
            current_dt = post_time_
        else:
            print(f"{post_time_} out of order")
            return False
    return True

def sort_table(db_path: Path):
    db = DatabaseManager.sqlite_db_from_path(db_path)
    session = db.Session()

    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info(f"Starting reordering of DBPost table by 'date_created'")

    # Get the engine from the session
    engine = session.bind
    metadata = MetaData()

    try:
        # Begin a transaction
        with engine.begin() as conn:
            # Reflect the DBPost table structure
            metadata.reflect(bind=engine, only=['post'])
            original_table = metadata.tables['post']

            # Create a new temporary table with the same structure
            temp_table_name = "temp_post"
            temp_table = Table(
                temp_table_name,
                metadata,
                *[column.copy() for column in original_table.columns]
            )

            # Create the temporary table
            temp_table.create(bind=conn)
            logger.info(f"Created temporary table {temp_table_name}")

            # Use a proper SQLAlchemy select statement for ordering
            from sqlalchemy import select

            # Create a select statement from the original table, ordered by date_created
            select_stmt = select(original_table).order_by(original_table.c.date_created)

            # Insert using the select statement
            insert_stmt = temp_table.insert().from_select(
                [c.name for c in original_table.columns],
                select_stmt
            )

            conn.execute(insert_stmt)
            logger.info("Inserted sorted data into temporary table")

            # # Drop original table
            # conn.execute(text("DROP TABLE post"))
            # logger.info("Dropped original DBPost table")
            #
            # # Rename temp table to original name
            # conn.execute(text(f"ALTER TABLE {temp_table_name} RENAME TO post"))
            # logger.info("Renamed temporary table to post")

        logger.info("Completed reordering DBPost table by date_created")
        return True

    except Exception as e:
        logger.error(f"Error reordering DBPost table: {e}")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    #check_sorted(Path("/home/rsoleyma/projects/platforms-clients/data/col_db/tiktok/tiktok.sqlite"), "temp_post")
    sort_table(Path("/home/rsoleyma/projects/platforms-clients/data/col_db/tiktok/tiktok.sqlite"))