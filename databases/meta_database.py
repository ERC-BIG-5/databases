from pathlib import Path

from databases import db_utils
from databases.db_stats import generate_db_stats
from databases.db_mgmt import DatabaseManager
from databases.db_models import DBPlatformDatabase2
from databases.external import DBConfig, SQliteConnection, MetaDatabaseContentModel
from tools.env_root import root


class MetaDatabase():

    def __init__(self, create: bool = False):
        self.db = DatabaseManager(config=DBConfig(
            db_connection=SQliteConnection(db_path=root() / "data/col_db/new_main.sqlite"),
            create=create,
            require_existing_parent_dir=True,
            tables=["platform_databases2"]
        ))
        self.db.init_database()

    def purge(self, simulate: bool = False):
        if simulate:
            print("SIMULATE")
        with self.db.get_session() as session:
            for db in session.query(DBPlatformDatabase2):
                if not Path(db.db_path).exists():
                    name = f"{db.name}: {db.db_path} does not exist"
                    print("Delete", name)
                    if not simulate:
                        session.delete(db)


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
        platforms = list(db_utils.check_platforms(db))
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
        tasks_states=db_utils.count_states(db),
        post_count=db_utils.count_posts(db=db),
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


def merge_into(src_path: Path, dest_path: Path, delete_after_full_merge: bool = True):
    # todo
    raise NotImplementedError()


def purge():
    """
    delete database-rows, which do not exist on the filesystem anymore
    :return:
    """


if __name__ == "__main__":
    root("/home/rsoleyma/projects/platforms-clients")

    MetaDatabase().purge(False)
    """
    meta_db = DatabaseManager(config=DBConfig(
        db_connection=SQliteConnection(db_path=root() / "data/col_db/new_main.sqlite"),
        create=True,
        require_existing_parent_dir=False,
        tables=["platform_databases2"]
    ))
    meta_db.init_database()
    """

    # add_db(root() / "data/youtube_backup_1112.sqlite", meta_db)

    # for db in (root() / "data").glob("*.sqlite"):
    #     add_db(db, meta_db)
    #     print("*****")

    # add_db(Path("/home/rsoleyma/projects/platforms-clients/data/col_db/twitter/twitter.sqlite"), meta_db)

    """
    from sqlalchemy import select

    with meta_db.get_session() as session:
        for p_db in session.execute(select(DBPlatformDatabase2)).scalars():
            m = p_db.model()
            print(f"{m.platform} {m.db_path} {m.content.post_count}")
    """
