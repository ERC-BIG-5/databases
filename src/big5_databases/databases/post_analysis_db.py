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
from big5_databases.databases.model_conversion import PostModel, PlatformDatabaseModel
from sqlalchemy.dialects.sqlite import insert, insert

TEMP_MAIN_DB = "/home/rsoleyma/projects/big5/platform_clients/data/dbs/main.sqlite"
BATCH_SIZE = 200

logger = get_logger(__file__)


def post_text(p: PostModel) -> dict[str, str]:
    match p.platform:
        case "youtube":
            return {"title": p.content["snippet"]["title"], "description": p.content["snippet"]["description"]}
        case "twitter":
            return {"text": p.content["rawContent"]}
        case "tiktok":
            return {"text": p.content["video_description"]}
        case "instagram":
            return {"text": p.content["text"]}
        case _:
            raise ValueError(f"unknown platform: {p.platform}")


def create_from_db(db: PlatformDatabaseModel, target_db: Path, input_data_method: Callable[[PostModel], dict | list]):
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
        for batch in tqdm(batched(session.query(DBPost).yield_per(BATCH_SIZE), BATCH_SIZE),total=expected_iter_count):
            batch_data = [(p.platform_id, input_data_method(p.model())) for p in batch]
            with target_db.get_session() as t_session:
                # todo, filter existing...
                t_session.bulk_save_objects([])

                for p in batch_data:
                    stmt = insert(DBPostProcessItem).values(platform_id=p[0], input=p[1])
                    stmt = stmt.on_conflict_do_nothing()
                    result = t_session.execute(stmt)
                    sum_inserted += result.rowcount
        # print(sum_inserted)
        logger.info(f"Added {sum_inserted} posts")


def create_packaged_databases(source_db_names: list[str],
                              destination_folder: Path,
                              input_data_method: Callable[[PostModel], dict | list],
                              source_meta_db: Optional[Path] = None,
                              delete_destination: bool = False
                              ):
    if not destination_folder.is_absolute():
        destination_folder = SqliteSettings().default_sqlite_dbs_base_path / destination_folder
        logger.info(f"Setting destination dir to {destination_folder}")

    if destination_folder.exists():
        if delete_destination:
            shutil.rmtree(destination_folder)
        else:
            raise ValueError(f"Destination exists already: {destination_folder}")

    meta_db = MetaDatabase(source_meta_db)
    missing_dbs = meta_db.check_all_databases()
    required_missing = list(filter(lambda db: db in missing_dbs, source_db_names))
    if required_missing:
        raise ValueError(f"Some databases are missing: {required_missing}")

    destination_folder.mkdir(parents=True)
    for db_name in tqdm(source_db_names):
        db = meta_db.get(db_name)
        dest_file = db.db_path.name
        create_from_db(db, destination_folder / dest_file, input_data_method)


if __name__ == "__main__":

    shutil.rmtree(Path(f"ana/a_test1"), ignore_errors=True)
    create_packaged_databases(["phase-2_youtube_es"],
                              Path(f"ana/a_test1"),
                              post_text,
                              Path(TEMP_MAIN_DB), delete_destination=True)
