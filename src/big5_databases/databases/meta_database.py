from datetime import datetime
from pathlib import Path
from typing import Optional, Callable

from sqlalchemy.exc import IntegrityError, NoResultFound
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.orm.session import Session

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

    def exists(self, id_: int | str | PlatformDatabaseModel) -> bool:
        return self[id_] is not None

    def get_db_mgmt(self, id_: int | str | PlatformDatabaseModel) -> Optional[DatabaseManager]:
        dbm = self[id_].get_mgmt()
        dbm.set_meta(self[id_])
        return dbm

    def __getitem__(self, id_: int | str | PlatformDatabaseModel) -> Optional[PlatformDatabaseModel]:
        return self.edit(id_)

    def edit(self, id_: int | str, func: Optional[Callable[[Session, DBPlatformDatabase], None]] = None):
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
            if func is None:
                def func_(session_, obj_):
                    return None

                func = func_
            func(session, db_obj)
            return db_obj.model()

    def move_database(self, id_: int | str, new_path: str | Path):
        def move_db(session, db: DBPlatformDatabase):
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

    def delete(self, id_: int | str):
        def del_db(session, db: DBPlatformDatabase):
            session.delete(db)

        self.edit(id_, del_db)

    def purge(self, simulate: bool = False):
        if simulate:
            print("SIMULATE")
        for db in self.get_dbs():
            if not Path(db.db_path).exists():
                name = f"{db.name}: {db.db_path} does not exist"
                print("Delete", name)
                if not simulate:
                    def del_db(session, db: DBPlatformDatabase):
                        session.delete(db)

                    self.edit(db.id, del_db)

    def general_databases_status(self, task_status: bool = True):
        task_status_types = ["done", "init", "paused", "aborted"] if task_status else []
        results = []

        # use a database
        dbs: list[PlatformDatabaseModel] = self.get_dbs()
        for db in dbs:
            row = {"name": db.name,
                   "platform": db.platform,
                   "path": str(db.db_path)}
            if db.exists():
                # print(db.name, db.content.file_size, int(db_utils.file_size(db)))
                running = db_utils.currently_open(db)
                if db.content.file_size != int(db_utils.file_size(db)) or running:
                    print(f"updating db stats for {db.name}")
                    self.update_db_base_stats(db)
                    if running:
                        row["name"] = f"[yellow]{row["name"]}[/yellow]"
                    else: # updated
                        row["name"] = f"[blue]{row["name"]}[/blue]"

                # todo hotfix for server. but need to test! and improve
                db_content = db.content
                if not db_content.last_modified:
                    # todo, not sure, why I need to re-assign
                    db = self.update_db_base_stats(db)
                    db_content = db.content

                print(db)
                row.update({
                    "last mod": f"{datetime.fromtimestamp(db_content.last_modified):%Y-%m-%d %H:%M}",
                    "total": str(db_content.post_count),
                    "size": f"{int(db_content.file_size / (1024 * 1024))} Mb"})
                row.update({k:str(db_content.tasks_states.get(k,0)) for k in task_status_types})
                # db.content.file_size = int(db_utils.file_size(db))

            else:
                row["path"] = f"[red]{row["path"]}[/red]"
            results.append(row)

        return results

    def update_db_base_stats(self, id_: int | str | PlatformDatabaseModel) -> PlatformDatabaseModel:
        if isinstance(id_, PlatformDatabaseModel):
            model = id_
        else:
            model = self[id_]
        model.content = model.get_mgmt().calc_db_content()

        def update_stats(session, db: DBPlatformDatabase):
            db.content = model.content.model_dump()
            flag_modified(db, "content")

        self.edit(id_, update_stats)
        return model


# todo kick out
def check_exists(path: str, metadb: DatabaseManager) -> bool:
    with metadb.get_session() as session:
        return session.query(DBPlatformDatabase).filter(DBPlatformDatabase.db_path == path).scalar() is not None


# todo, outdated stuff.. redo
def add_db(path: str | Path, metadb: DatabaseManager, update: bool = False) :
    # todo...
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

    # todo, generate_db_stats is deprecated
    # try:
    #     stats = generate_db_stats(db)
    # except Exception as err:
    #     print(f"skipping {full_path_str}")
    #     print(f"  {err}")
    #     return

    with metadb.get_session() as session:
        meta_db_entry = DBPlatformDatabase(
            db_path=db_path.absolute().as_posix(),
            platform=platforms[0],
            is_default=False,
            content=db.calc_db_content().model_dump()
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


