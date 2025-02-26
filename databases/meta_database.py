from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field

from databases import db_utils
from databases.db_stats import DBStats, generate_db_stats
from databases.db_mgmt import DatabaseManager
from databases.db_models import DBPlatformDatabase2
from databases.db_utils import check_platforms, count_posts
from databases.external import DBConfig, SQliteConnection
from tools.env_root import root


class MetaDatabaseContentModel(BaseModel):
    tasks_states: dict[str, int] = Field(default_factory=dict)
    post_count: int
    file_size: int
    stats: DBStats
    annotation: Optional[str] = None


def check_exists(path: str, metadb: DatabaseManager) -> bool:
    with metadb.get_session() as session:
        return session.query(DBPlatformDatabase2).filter(DBPlatformDatabase2.db_path == path).scalar() is not None


def add_db(path: str | Path, metadb: DatabaseManager, update: bool = False):
    db_path = Path(path)
    full_path_str = db_path.absolute().as_posix()
    if check_exists(full_path_str, metadb):
        if not update:
            print(f"{full_path_str} already exists, skipping.")
            return
    db = DatabaseManager.sqlite_db_from_path(db_path)
    try:
        platforms = list(check_platforms(db))
    except Exception as err:
        print(f"skipping {full_path_str}")
        print(f"  {err}")
        return

    if len(platforms) == 0:
        print(f"db empty. NOT ADDING: {path}")
        return
    if len(platforms) > 1:
        print(f"db multiple platforms: {platforms}. NOT ADDING: {path}")
        return

    try:
        stats = generate_db_stats(db)
    except Exception as err:
        print(f"skipping {full_path_str}")
        print(f"  {err}")
        return

    content = MetaDatabaseContentModel(
        tasks_states=db.count_states(),
        post_count=count_posts(db_manager=db),
        file_size=db_utils.file_size(db),
        stats=stats)

    with metadb.get_session() as session:
        meta_db_entry = DBPlatformDatabase2(
            db_path=db_path.absolute().as_posix(),
            platform=platforms[0],
            is_default=False,
            content=content.model_dump()
        )
        session.add(meta_db_entry)


if __name__ == "__main__":
    root("/home/rsoleyma/projects/platforms-clients")
    meta_db = DatabaseManager(config=DBConfig(
        db_connection=SQliteConnection(db_path=root() / "data/col_db/new_main.sqlite"),
        create=True,
        require_existing_parent_dir=False,
        tables=["platform_databases2"]
    ))
    meta_db.init_database()

    # add_db(root() / "data/youtube_backup_1112.sqlite", meta_db)

    """
    for db in (root() / "data").glob("*.sqlite"):
        add_db(db, meta_db)
        print("*****")
    """
    from sqlalchemy import select
    with meta_db.get_session() as session:
        for p_db in session.execute(select(DBPlatformDatabase2)).scalars():
            print(f"{p_db.platform} {p_db.db_path} {p_db.content['post_count']}")