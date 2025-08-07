"""
This function checks databases and updates them to the next version (3)
In version 3, posts do not include the date_collected columns, but rather the collection_tasks do

vibe coded. not yet tested...
"""

import logging
from pathlib import Path

from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker

from big5_databases.databases.db_mgmt import DatabaseManager
from big5_databases.databases.db_models import DBPost, DBCollectionTask


def migrate_date_collected_column(db_path):
    """
    Migrates date_collected from post table to collection_task table.

    For each collection_task, takes the date_collected from the first post
    and moves it to the collection_task table, then removes the column from post.

    Args:
        db_path (str): Path to SQLite database file

    Returns:
        dict: Status report of the migration
    """

    # Setup logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Create engine and session
    engine = create_engine(f'sqlite:///{db_path}')
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Get inspector to check table structure
        inspector = inspect(engine)

        # Check if tables exist
        tables = inspector.get_table_names()
        if 'post' not in tables or 'collection_task' not in tables:
            return {"error": "Required tables (post, collection_task) not found"}

        # Check if date_collected exists in post table
        post_columns = [col['name'] for col in inspector.get_columns('post')]
        if 'date_collected' not in post_columns:
            return {"error": "date_collected column not found in post table"}

        # Check if date_collected already exists in collection_task table
        collection_task_columns = [col['name'] for col in inspector.get_columns('collection_task')]

        # Step 1: Add date_collected column to collection_task if it doesn't exist
        if 'execution_ts' not in collection_task_columns:
            logger.info("Adding execution_ts column to collection_task table")
            session.execute(text("""
                                 ALTER TABLE collection_task
                                     ADD COLUMN execution_ts DATETIME
                                 """))
            session.commit()

        # Step 2: Get the first post's date_collected for each collection_task
        logger.info("Getting first post date for each collection_task")

        # Query to get the first post's date_collected for each collection_task
        # Assuming post has a foreign key column like 'collection_task_id'
        result = session.execute(text("""
                                      SELECT p.collection_task_id,
                                             MIN(p.date_collected) as first_date_collected
                                      FROM post p
                                      WHERE p.date_collected IS NOT NULL
                                      GROUP BY p.collection_task_id
                                      """)).fetchall()

        if not result:
            return {"error": "No posts with date_collected found"}

        # Step 3: Update collection_task with the first post's date_collected
        logger.info(f"Updating {len(result)} collection_task records")

        updated_count = 0
        for row in result:
            collection_task_id, first_date = row
            session.execute(text("""
                                 UPDATE collection_task
                                 SET execution_ts = :date_collected
                                 WHERE id = :task_id
                                 """), {
                                'date_collected': first_date,
                                'task_id': collection_task_id
                            })
            updated_count += 1

        session.commit()

        # Step 4: Verify the migration worked
        verification = session.execute(text("""
                                            SELECT COUNT(*)
                                            FROM collection_task
                                            WHERE execution_ts IS NOT NULL
                                            """)).scalar()

        logger.info(f"Verified: {verification} collection_task records have execution_ts")

        # Step 5: Drop the date_collected column from post table
        logger.info("Dropping date_collected column from post table")

        # SQLite doesn't support DROP COLUMN directly, so we need to recreate the table
        # First, get the post table structure (excluding date_collected)
        post_columns_info = inspector.get_columns('post')
        columns_to_keep = [col for col in post_columns_info if col['name'] != 'date_collected']

        # Create column definitions for the new table
        column_defs = []
        for col in columns_to_keep:
            col_def = f"{col['name']} {col['type']}"
            if not col['nullable']:
                col_def += " NOT NULL"
            if col.get('default'):
                col_def += f" DEFAULT {col['default']}"
            column_defs.append(col_def)

        # Get foreign key constraints
        foreign_keys = inspector.get_foreign_keys('post')
        fk_defs = []
        for fk in foreign_keys:
            fk_def = f"FOREIGN KEY ({', '.join(fk['constrained_columns'])}) REFERENCES {fk['referred_table']}({', '.join(fk['referred_columns'])})"
            fk_defs.append(fk_def)

        # Combine columns and foreign keys
        all_defs = column_defs + fk_defs

        # Execute the table recreation
        session.execute(text("BEGIN TRANSACTION"))

        # Create new table
        create_table_sql = f"""
            CREATE TABLE post_new (
                {', '.join(all_defs)}
            )
        """
        session.execute(text(create_table_sql))

        # Copy data (excluding date_collected)
        columns_to_copy = [col['name'] for col in columns_to_keep]
        copy_sql = f"""
            INSERT INTO post_new ({', '.join(columns_to_copy)})
            SELECT {', '.join(columns_to_copy)}
            FROM post
        """
        session.execute(text(copy_sql))

        # Drop old table and rename new one
        session.execute(text("DROP TABLE post"))
        session.execute(text("ALTER TABLE post_new RENAME TO post"))

        session.execute(text("COMMIT"))

        logger.info("Successfully migrated date_collected column")

        return {
            "success": True,
            "collection_tasks_updated": updated_count,
            "message": f"Successfully migrated date_collected from post to collection_task for {updated_count} tasks"
        }

    except Exception as e:
        session.rollback()
        logger.error(f"Migration failed: {str(e)}")
        return {"error": f"Migration failed: {str(e)}"}

    finally:
        session.close()

def add_platform_collection_config_col(db_path):
    """
    """
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Create engine and session
    engine = create_engine(f'sqlite:///{db_path}')
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Get inspector to check table structure
        inspector = inspect(engine)

        # Check if tables exist
        tables = inspector.get_table_names()
        if 'post' not in tables or 'collection_task' not in tables:
            return {"error": "Required tables (post, collection_task) not found"}

        task_columns = [col['name'] for col in inspector.get_columns('collection_task')]
        if 'platform_collection_config' not in task_columns:
            print("work todo")

            session.execute(text("""
                                 ALTER TABLE collection_task
                                     ADD COLUMN platform_collection_config JSON
                                 """))
            session.commit()


        db = DatabaseManager.sqlite_db_from_path(db_path)
        task = session.query(DBCollectionTask).first()

    except Exception as e:
        session.rollback()
        logger.error(f"Migration failed: {str(e)}")
        return {"error": f"Migration failed: {str(e)}"}

    finally:
        session.close()

def check_migration(db_path):
    db_mgmt = DatabaseManager.sqlite_db_from_path(db_path)
    with db_mgmt.get_session() as session:
        post = session.query(DBPost).first()
        print(post.model().model_dump().keys())

        task = session.query(DBCollectionTask).first()
        # print(task.model().model_dump().keys())


# Example usage
if __name__ == "__main__":
    p1 = Path("/home/rsoleyma/projects/big5/platform_clients/data/dbs/youtube.sqlite")
    migrate_date_collected_column(str(p1))
    # add_platform_collection_config_col(p1)
    # check_migration(Path("/home/rsoleyma/projects/big5/platform_clients/data/dbs/tiktok.sqlite"))
#     # Replace with your actual database path
#     db_path = "your_database.db"
#
#     result = migrate_date_collected_column(db_path)
#
#     if result.get("success"):
#         print("✅ Migration completed successfully!")
#         print(f"   {result['message']}")
#     else:
#         print("❌ Migration failed!")
#         print(f"   Error: {result['error']}")