import math
import shutil

from itertools import batched
from pathlib import Path
from typing import Callable, Optional

from tqdm.std import tqdm
from tools.project_logging import get_logger

from big5_databases.databases.db_mgmt import DatabaseManager
from big5_databases.databases.db_models import DBPost, DBPostProcessItem
from big5_databases.databases.db_settings import SqliteSettings
from big5_databases.databases.external import DBConfig, SQliteConnection
from big5_databases.databases.meta_database import MetaDatabase
from big5_databases.databases.model_conversion import PlatformDatabaseModel
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy import select

TEMP_MAIN_DB = "/home/rsoleyma/projects/big5/platform_clients/data/dbs/main.sqlite"
BATCH_SIZE = 200

logger = get_logger(__file__)


def post_text(platform: str, content: dict, metadata_content: dict = None) -> dict[str, str]:
    match platform:
        case "youtube":
            return {"title": content["snippet"]["title"], "description": content["snippet"]["description"]}
        case "twitter":
            return {"text": content["rawContent"]}
        case "tiktok":
            return {"text": content["video_description"]}
        case "instagram":
            return {"text": content["text"]}
        case _:
            raise ValueError(f"unknown platform: {platform}")


def create_from_db(db: PlatformDatabaseModel, target_db: Path, input_data_method: Callable[[str, dict, dict], dict | list]):
    mgmt = db.get_mgmt()

    target_db = DatabaseManager(DBConfig(name=db.name,
                                         create=True,
                                         require_existing_parent_dir=False,
                                         tables=["ppitem"],
                                         db_connection=SQliteConnection(db_path=target_db)))

    post_count = db.content.post_count
    expected_iter_count = math.ceil(post_count/ BATCH_SIZE)
    logger.info(f"Estimated batches: {expected_iter_count}")
    with mgmt.get_session() as session:
        # todo, maybe just, "content", metadata_content"
        sum_inserted = 0
        query = session.query(DBPost.platform_id, DBPost.platform, DBPost.content, DBPost.metadata_content).yield_per(BATCH_SIZE)
        for batch in tqdm(batched(query, BATCH_SIZE), total=expected_iter_count):
            # Extract platform_ids from the batch
            batch_platform_ids = [row.platform_id for row in batch]
            
            with target_db.get_session() as t_session:
                # Filter out existing platform_ids to avoid processing duplicates
                existing_ids = t_session.execute(
                    select(DBPostProcessItem.platform_id).filter(
                        DBPostProcessItem.platform_id.in_(batch_platform_ids)
                    )
                ).scalars().all()
                
                # Only process posts that don't already exist
                filtered_posts = [row for row in batch if row.platform_id not in existing_ids]
                
                # Now run the expensive input_data_method only on new posts
                batch_data = [(row.platform_id, input_data_method(row.platform, row.content, row.metadata_content)) for row in filtered_posts]

                for p in batch_data:
                    stmt = insert(DBPostProcessItem).values(platform_id=p[0], input=p[1])
                    result = t_session.execute(stmt)
                    sum_inserted += result.rowcount
        # print(sum_inserted)
        logger.info(f"Added {sum_inserted} posts")


def create_packaged_databases(source_db_names: list[str],
                              destination_folder: Path,
                              input_data_method: Callable[[str, dict, dict], dict | list],
                              source_meta_db: Optional[Path] = None,
                              delete_destination: bool = False,
                              exists_ok: bool = False
                              ):
    if not destination_folder.is_absolute():
        destination_folder = SqliteSettings().default_sqlite_dbs_base_path / destination_folder
        logger.info(f"Setting destination dir to {destination_folder}")

    if destination_folder.exists():
        if delete_destination:
            shutil.rmtree(destination_folder)
        elif not exists_ok:
            raise ValueError(f"Destination exists already: {destination_folder}")
        # If exists_ok=True, continue without removing existing folder

    meta_db = MetaDatabase(source_meta_db)
    missing_dbs = meta_db.check_all_databases()
    required_missing = list(filter(lambda db: db in missing_dbs, source_db_names))
    if required_missing:
        raise ValueError(f"Some databases are missing: {required_missing}")

    destination_folder.mkdir(parents=True, exist_ok=True)
    for db_name in tqdm(source_db_names):
        db = meta_db.get(db_name)
        dest_file = db.db_path.name
        create_from_db(db, destination_folder / dest_file, input_data_method)

def add_db_to_package(db_name: str,
                      destination_folder: Path,
                      input_data_method: Callable[[str, dict, dict], dict | list],
                      source_meta_db: Optional[Path] = None,
                      exists_ok: bool = True):
    if not destination_folder.is_absolute():
        destination_folder = SqliteSettings().default_sqlite_dbs_base_path / destination_folder
        logger.info(f"Setting destination dir to {destination_folder}")

    if not destination_folder.exists():
        if exists_ok:
            destination_folder.mkdir(parents=True, exist_ok=True)
        else:
            raise ValueError(f"Destination folder missing: {destination_folder}")

    meta_db = MetaDatabase(source_meta_db)
    missing_dbs = meta_db.check_all_databases()
    if db_name in missing_dbs:
        raise ValueError(f"database missing: {db_name}")

    db = meta_db.get(db_name)
    dest_file = db.db_path.name
    create_from_db(db, destination_folder / dest_file, input_data_method)

if __name__ == "__main__":

    #shutil.rmtree(Path(f"ana/a_test1"), ignore_errors=True)
    create_packaged_databases(["phase-2_youtube_es"],
                              Path(f"ana/a_test1"),
                              post_text,
                              Path(TEMP_MAIN_DB), delete_destination=False, exists_ok=True)
