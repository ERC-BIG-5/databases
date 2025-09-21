"""
not sure if we want to do that... maybe just use alt-paths... and merge...
"""
import math
import shutil

from itertools import batched
from pathlib import Path
from typing import Callable, Optional, Type

from pydantic import BaseModel, ValidationError

from tqdm.std import tqdm
from tools.project_logging import get_logger

from big5_databases.databases.db_mgmt import DatabaseManager
from big5_databases.databases.db_models import DBPost, DBPostProcessItem
from big5_databases.databases.db_settings import SqliteSettings
from big5_databases.databases.external import DBConfig, SQliteConnection
from big5_databases.databases.meta_database import MetaDatabase
from big5_databases.databases.model_conversion import PlatformDatabaseModel
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy import select, func
from sqlalchemy.orm.attributes import flag_modified

try:
    import torch
    from torch.utils.data import Dataset

    has_datasets = True
except ImportError:
    has_datasets = False

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


def merge_back_analysis_results(
        analysis_folder: Path,
        analysis_key: str,
        output_model: Type[BaseModel],
        overwrite: bool = False,
        source_meta_db: Optional[Path] = None,
        batch_size: int = 200
) -> dict[str, dict[str, int]]:
    """
    Merge analysis results back into source databases by updating metadata_content.
    
    Args:
        analysis_folder: Path to folder containing analysis databases
        analysis_key: Key to store results under in metadata_content
        output_model: Pydantic model to validate output data
        overwrite: Whether to overwrite existing keys
        source_meta_db: Optional path to meta database
        batch_size: Batch size for processing
        
    Returns:
        Dictionary with stats per database: {db_name: {updated, skipped, errors}}
    """
    if not analysis_folder.is_absolute():
        analysis_folder = SqliteSettings().default_sqlite_dbs_base_path / analysis_folder

    meta_db = MetaDatabase(source_meta_db)
    all_stats = {}

    # Process each analysis database
    for analysis_db_file in analysis_folder.glob("*.sqlite"):
        db_name = analysis_db_file.stem
        stats = {"updated": 0, "skipped": 0, "errors": 0}

        # Find matching source database
        # todo. getter does not use the db FILE-name  unfortunately
        try:
            source_db = meta_db.get(db_name)
        except KeyError:
            logger.warning(f"Source database '{db_name}' not found in meta database")
            continue

        # Set up database connections
        analysis_db_mgmt = DatabaseManager(DBConfig(
            db_connection=SQliteConnection(db_path=analysis_db_file),
            tables=["ppitem"]
        ))
        source_db_mgmt = source_db.get_mgmt()

        # Count total rows for progress
        with analysis_db_mgmt.get_session() as analysis_session:
            total_rows = analysis_session.query(func.count(DBPostProcessItem.platform_id)).filter(
                DBPostProcessItem.output.isnot(None)
            ).scalar()

            if total_rows == 0:
                logger.info(f"No results to merge for database '{db_name}'")
                all_stats[db_name] = stats
                continue

            # Process in batches
            query = analysis_session.query(
                DBPostProcessItem.platform_id,
                DBPostProcessItem.output
            ).filter(DBPostProcessItem.output.isnot(None)).yield_per(batch_size)

            with tqdm(total=total_rows, desc=f"Merging {db_name}") as pbar:
                for batch in batched(query, batch_size):
                    with source_db_mgmt.get_session() as source_session:
                        for platform_id, output_data in batch:
                            try:
                                # Validate output against model
                                validated_output = output_model.model_validate(output_data)
                            except ValidationError as e:
                                logger.error(f"Invalid output for {platform_id} in {db_name}: {e}")
                                stats["errors"] += 1
                                pbar.update(1)
                                continue

                            # Find source row
                            source_row = source_session.query(DBPost).filter_by(platform_id=platform_id).first()
                            if not source_row:
                                logger.warning(f"Platform ID {platform_id} not found in source database {db_name}")
                                stats["errors"] += 1
                                pbar.update(1)
                                continue

                            # Check existing key
                            if analysis_key in source_row.metadata_content:
                                if not overwrite:
                                    stats["skipped"] += 1
                                    pbar.update(1)
                                    continue

                            # Update metadata_content
                            source_row.metadata_content[analysis_key] = validated_output.model_dump()
                            flag_modified(source_row, "metadata_content")
                            stats["updated"] += 1
                            pbar.update(1)

                        # Commit batch
                        # todo this happens automatically
                        source_session.commit()

        all_stats[db_name] = stats
        logger.info(
            f"Merged {db_name}: {stats['updated']} updated, {stats['skipped']} skipped, {stats['errors']} errors")

    return all_stats


def _create_from_db(db: PlatformDatabaseModel, target_db: Path,
                    input_data_method: Callable[[str, dict, dict], dict | list]):
    mgmt = db.get_mgmt()

    target_db = DatabaseManager(DBConfig(name=db.name,
                                         create=True,
                                         require_existing_parent_dir=False,
                                         tables=["ppitem"],
                                         db_connection=SQliteConnection(db_path=target_db)))

    post_count = db.content.post_count
    expected_iter_count = math.ceil(post_count / BATCH_SIZE)
    logger.info(f"Estimated batches: {expected_iter_count}")
    with mgmt.get_session() as session:
        # todo, maybe just, "content", metadata_content"
        sum_inserted = 0
        query = session.query(DBPost.platform_id, DBPost.platform, DBPost.content, DBPost.metadata_content).yield_per(
            BATCH_SIZE)
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
                batch_data = [(row.platform_id, input_data_method(row.platform, row.content, row.metadata_content)) for
                              row in filtered_posts]

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
        _create_from_db(db, destination_folder / dest_file, input_data_method)


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
    _create_from_db(db, destination_folder / dest_file, input_data_method)


if has_datasets:
    class SQLiteDataset(Dataset):
        def __init__(self, db_path, query, transform=None):
            self.db_path = db_path
            self.transform = transform

            # Load data from SQLite
            # conn = sqlite3.connect(db_path)
            # self.data = pd.read_sql_query(query, conn)
            # conn.close()

        def __len__(self):
            return len(self.data)

        def __getitem__(self, idx):
            row = self.data.iloc[idx]

            # Convert to tensors or apply transforms as needed
            if self.transform:
                row = self.transform(row)

            return row
else:
    class SQLiteDataset():
        def __init__(self, db_path, query, transform=None):
            raise ValueError("Cannot use SQLiteDataset without datasets package")

"""

# Usage
dataset = SQLiteDataset(
    db_path="your_database.db",
    query="SELECT * FROM your_table"
)
dataloader = torch.utils.data.DataLoader(dataset, batch_size=32, shuffle=True)
"""

if __name__ == "__main__":
    # Example usage for creating analysis databases
    # shutil.rmtree(Path(f"ana/a_test1"), ignore_errors=True)
    create_packaged_databases(["phase-2_youtube_es"],
                              Path(f"ana/a_test1"),
                              post_text,
                              Path(TEMP_MAIN_DB), delete_destination=False, exists_ok=True)

    # Example usage for merging back results
    # class SentimentResult(BaseModel):
    #     score: float
    #     label: str
    #     confidence: float
    #
    # stats = merge_back_analysis_results(
    #     analysis_folder=Path("ana/sentiment_analysis"),
    #     analysis_key="sentiment",
    #     output_model=SentimentResult,
    #     overwrite=False,
    #     source_meta_db=Path(TEMP_MAIN_DB)
    # )
    # print(stats)
