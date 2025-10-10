"""
Database table sorting utilities for chronological ordering.

This module provides functions to check and sort database tables by date_created
column, ensuring proper chronological ordering of posts and other time-based data.
The sorting operations use SQLAlchemy's safe table manipulation methods.
"""

import logging
from datetime import datetime
from pathlib import Path

from sqlalchemy import Table, MetaData, text

from big5_databases.databases.db_mgmt import DatabaseManager


def check_sorted(db_path: Path, table: str = "post") -> bool:
    """
    Check if a database table is sorted by date_created column.

    Parameters
    ----------
    db_path : Path
        Path to the SQLite database file.
    table : str, optional
        Name of the table to check, by default "post".

    Returns
    -------
    bool
        True if the table is sorted in ascending order by date_created,
        False if any records are out of chronological order.

    Notes
    -----
    This function iterates through all records in the table and verifies
    that each date_created value is greater than or equal to the previous one.
    If any record is found to be out of order, it prints the problematic
    timestamp and returns False.

    The function assumes date_created values are stored as ISO format strings
    that can be parsed by datetime.fromisoformat().
    """
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

def sort_table(db_path: Path) -> bool:
    """
    Sort a database table by date_created column using a temporary table approach.

    Parameters
    ----------
    db_path : Path
        Path to the SQLite database file containing the table to sort.

    Returns
    -------
    bool
        True if the sorting operation completed successfully.

    Raises
    ------
    Exception
        If any database operation fails during the sorting process.

    Notes
    -----
    This function implements a safe table sorting strategy:

    1. Creates a temporary table with the same structure as the original 'post' table
    2. Inserts all data from the original table into the temporary table,
       ordered by date_created in ascending order
    3. The original table replacement step is currently commented out for safety

    The function uses SQLAlchemy's table reflection and proper SQL operations
    to ensure data integrity during the sorting process. All operations are
    performed within a database transaction for atomicity.

    Currently, the final steps (dropping original table and renaming temp table)
    are commented out as a safety measure. Uncomment lines 78-84 to complete
    the table replacement after verifying the temporary table contains correct data.

    Warning
    -------
    This operation modifies the database structure. Always backup your database
    before running this function on production data.
    """
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
    # Example usage - replace with your actual database path
    # check_sorted(Path("/path/to/your/database.sqlite"), "temp_post")
    # sort_table(Path("/path/to/your/database.sqlite"))
    pass