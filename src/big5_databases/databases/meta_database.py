from pathlib import Path
from typing import Optional, Callable

from sqlalchemy.exc import IntegrityError, NoResultFound

from big5_databases.databases import db_utils
from big5_databases.databases.db_utils import count_posts
from big5_databases.databases.model_conversion import PlatformDatabaseModel
from big5_databases.databases.settings import SETTINGS
from .db_models import DBPlatformDatabase
from .db_stats import generate_db_stats
from .db_mgmt import DatabaseManager
from .external import DBConfig, SQliteConnection, MetaDatabaseContentModel
from tools.env_root import root
from tools.project_logging import get_logger

logger = get_logger(__file__)


class MetaDatabase:

    def __init__(self, db_path: Optional[Path] = None, create: bool = False):
        if not db_path:
            if SETTINGS.main_db_path:
                db_path = Path(SETTINGS.meta_db_path)
            else:
                db_path = root() / "data/dbs/main.sqlite"

        if create:
            db_path.parent.mkdir(parents=True, exist_ok=True)

        self.db = DatabaseManager(config=DBConfig(
            db_connection=SQliteConnection(db_path=db_path),
            create=create,
            require_existing_parent_dir=True,
            tables=["platform_databases"],
        ))
        # self.db.init_database()

    def get_dbs(self) -> list[PlatformDatabaseModel]:
        """Get all registered platforms from the main database"""
        with self.db.get_session() as session:
            return [o.model() for o in session.query(DBPlatformDatabase).all()]

    def get_db_mgmt(self, id_: int | str | PlatformDatabaseModel) -> Optional[DatabaseManager]:
        dbm = self[id_]
        if not dbm.full_path.exists():
            raise ValueError(f"Could not load database {id_} from meta-database")
        return DatabaseManager.sqlite_db_from_path(dbm.db_path).set_meta(dbm)

    def __getitem__(self, id_: int | str | PlatformDatabaseModel) -> Optional[PlatformDatabaseModel]:
        return self.edit(id_)

    def edit(self, id_: int | str , func: Optional[Callable[[DBPlatformDatabase], None]] = lambda x: None):
        with self.db.get_session() as session:
            try:
                if isinstance(id_, PlatformDatabaseModel):
                    id_ = id_.id
                if isinstance(id_, int):
                    db_obj = session.query(DBPlatformDatabase).where(DBPlatformDatabase.id == id_).one()
                else:
                    db_obj = session.query(DBPlatformDatabase).where(DBPlatformDatabase.name == id_).one()
            except NoResultFound as err:
                logger.warning(f"Could not load database {db_obj.name} from meta-database")
                return None
            func(db_obj)
            return db_obj.model()

    def move_database(self, id_: int|str, new_path: str | Path):
        def move_db(db: DBPlatformDatabase):
            db.db_path = str(new_path)
        self.edit(id_, move_db)

    def add_db(self, db: PlatformDatabaseModel) -> bool:
        try:
            with self.db.get_session() as session:
                session.add(DBPlatformDatabase(
                    db_path=str(db.db_path.absolute()),
                    name=db.name,
                    platform=db.platform,
                    is_default=db.is_default,
                    content=db.content.model_dump()
                ))
        except IntegrityError as e:
            logger.error(f"Could not add database {db.name} to meta-database: {e.orig}")
            session.rollback()
            return False
        return True

    def purge(self, simulate: bool = False):
        if simulate:
            print("SIMULATE")
        with self.db.get_session() as session:
            for db in session.query(DBPlatformDatabase):
                if not Path(db.db_path).exists():
                    name = f"{db.name}: {db.db_path} does not exist"
                    print("Delete", name)
                    if not simulate:
                        session.delete(db)

    def general_databases_status(self, task_status: bool = True):

        task_status_types = ["done", "init", "paused", "aborted"] if task_status else []
        results = []

        def calc_row(db: DatabaseManager) -> dict[str, str | int]:
            if task_status:
                tasks = db_utils.count_states(db)
                status_numbers = [str(tasks.get(t, 0)) for t in task_status_types]
            else:
                status_numbers = []
            total_posts = str(count_posts(db=db))
            size = str(f"{int(db_utils.file_size(db) / (1024 * 1024))} Mb")
            return {"total": total_posts,
                    "size": size} | dict(zip(task_status_types, status_numbers))

        # use a database
        dbs = self.get_dbs()
        comon_path = dbs[0].full_path
        for db in dbs:
            c_p = db.full_path
            while not c_p.is_relative_to(comon_path):
                comon_path = comon_path.parent
                if str(comon_path) == ".":
                    comon_path = Path("/")
        for db in dbs:
            row = {"name": db.name, "platform": db.platform, "path": str(db.full_path.relative_to(comon_path))}
            try:
                db_mgmt: Optional[DatabaseManager] = self.get_db_mgmt(db)
                row.update(calc_row(db_mgmt))
            except ValueError:
                row["path"] = f"[red]{row["path"]}[/red]"
            results.append(row)
        return results


def check_exists(path: str, metadb: DatabaseManager) -> bool:
    with metadb.get_session() as session:
        return session.query(DBPlatformDatabase).filter(DBPlatformDatabase.db_path == path).scalar() is not None


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
        meta_db_entry = DBPlatformDatabase(
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
