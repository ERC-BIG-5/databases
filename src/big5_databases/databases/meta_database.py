import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Callable, Literal

from sqlalchemy.exc import IntegrityError, NoResultFound
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.orm.session import Session
from tools.env_root import root
from tools.project_logging import get_logger

from big5_databases.databases import db_utils
from big5_databases.databases.model_conversion import PlatformDatabaseModel
from .db_mgmt import DatabaseManager
from .db_models import DBPlatformDatabase
from .db_settings import SETTINGS
from .external import DBConfig, SQliteConnection, MetaDatabaseContentModel, DatabaseRunState
from .db_settings import SETTINGS, SqliteSettings
from .db_stats import generate_db_stats
from .db_mgmt import DatabaseManager
from .external import MetaDatabaseStatsModel, MetaDatabaseConfigModel
from .external import DBConfig, SQliteConnection, MetaDatabaseContentModel
from tools.env_root import root
from tools.project_logging import get_logger

logger = get_logger(__file__)


class MetaDatabase:

    def __init__(self, db_path: Optional[Path] = None, create: bool = False, check_databases: bool = True):
        if not db_path:
            if SETTINGS.main_db_path:
                db_path = Path(SETTINGS.main_db_path)
            else:
                db_path = root() / "data/dbs/main.sqlite"

        if create:
            db_path.parent.mkdir(parents=True, exist_ok=True)

        self.db = DatabaseManager(config=DBConfig(
            db_connection=SQliteConnection(db_path=db_path),
            name="meta",
            create=create,
            require_existing_parent_dir=True,
            tables=["platform_databases"],
        ))
        # self.db.init_database()
        if check_databases:
            missing_dbs = self.check_all_databases()
            if missing_dbs:
                logger.warning(f"Metadatabase contains database that does not exist: {missing_dbs}")

    def check_all_databases(self) -> list[str]:
        """
        check if all databases exists or return those missing (paths)
        """
        return [db.name for db in self.get_dbs() if not db.exists()]

    def get_dbs(self) -> list[PlatformDatabaseModel]:
        """Get all registered platforms from the main database"""
        with self.db.get_session() as session:
            return [o.model() for o in session.query(DBPlatformDatabase).all()]

    def exists(self, id_: int | str | PlatformDatabaseModel) -> bool:
        return self[id_] is not None

    def get_db_mgmt(self, id_: int | str | PlatformDatabaseModel) -> Optional[DatabaseManager]:
        db = self.get(id_)
        dbm = db.get_mgmt(db)
        return dbm

    def __getitem__(self, id_: int | str | PlatformDatabaseModel) -> Optional[PlatformDatabaseModel]:
        return self.edit(id_)

    def get(self, id_: int | str | PlatformDatabaseModel) -> PlatformDatabaseModel:
        db = self[id_]
        if not db:
            raise ValueError(f"Database : {id_} does not exist")
        return db

    def get_obj(self, session, id_: int | str) -> Optional[DBPlatformDatabase]:
        try:
            if isinstance(id_, PlatformDatabaseModel):
                id_ = id_.id
            if isinstance(id_, int):
                db_obj = session.query(DBPlatformDatabase).where(DBPlatformDatabase.id == id_).one()
            else:
                db_obj = session.query(DBPlatformDatabase).where(DBPlatformDatabase.name == id_).one()
        except NoResultFound as err:
            if isinstance(id_, PlatformDatabaseModel):
                name = id_.name
            else:
                name = id_
            from tools.fast_levenhstein import levenhstein_get_closest_matches
            similar = levenhstein_get_closest_matches(name, self.get_db_names(), threshold=0.8)
            raise ValueError(f"Could not load database {id_} from meta-database. Candidates: {similar}")
        return db_obj

    def edit(self,
             id_: int | str | PlatformDatabaseModel,
             func: Optional[Callable[[Session, DBPlatformDatabase], None]] = None,
             model: Optional[bool] = True) -> Optional[PlatformDatabaseModel]:
        with self.db.get_session() as session:
            db_obj = self.get_obj(session, id_)
            if func is None:
                def func_(session_, obj_):
                    return None

                func = func_
            func(session, db_obj)
            return db_obj.model()

    def set_db_path(self, id_: int | str, new_path: Path):

        # check if path exists, either if its absolute or relative to the default path
        if new_path.is_absolute() and not new_path.exists() and not (
                SETTINGS.default_sqlite_dbs_base_path / new_path).exists():
            raise ValueError(f"No database at location: {new_path}")

        def _set_db_path(session_: Session, db_obj: DBPlatformDatabase):
            db_obj.db_path = str(new_path)

        self.edit(id_, _set_db_path)

    def add_db(self, db: PlatformDatabaseModel, client_setup: Optional["ClientSetup"] = None) -> bool:
        try:
            with self.db.get_session() as session:
                # Store client_setup in content if provided
                content = db.content.model_dump()
                if client_setup:
                    # Exclude computed fields to avoid validation errors
                    content["client_setup"] = client_setup.model_dump(exclude={"db": {"connection_str", "db_type"}})

                # Validate content dict against MetaDatabaseContentModel before insertion
                validated_content = MetaDatabaseContentModel.model_validate(content)

                session.add(DBPlatformDatabase(
                    db_path=str(db.db_path),
                    name=db.name,
                    platform=db.platform,
                    is_default=db.is_default,
                    content=validated_content.model_dump()
                ))
            self.update_db_base_stats(db.name)
        except IntegrityError as e:
            logger.error(f"Could not add database {db.name} to meta-database: {e.orig}")
            session.rollback()
            return False
        return True

    def delete(self, id_: int | str):
        """
        delete a database
        """
        print("dell")
        # this is more robust cuz it also removes broken dbs that dont validate to the model
        full_path = None
        with self.db.get_session() as session:
            db = self.get_obj(session, id_)
            alt_paths = db.content["alternative_paths"]
            full_path = db.full_path
            session.delete(db)

        delete_file = input("Delete the file: [y] or mark?")

        if not full_path.exists():
            print("Database file not exist: '{str(p)}', so there is nothing more todo")

        if delete_file == "y":
            full_path.unlink()
        else:
            full_path.rename(full_path.parent / f"DEL_{full_path.db_path.name}")
        if alt_paths:
            print(
                f"Consider also the alternative database paths:\n{json.dumps({k: str(v) for k, v in alt_paths.items()}, indent=2)}")

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

    def general_databases_status(self,
                                 databases: Optional[list[str]] = None,
                                 task_status: bool = True,
                                 force_refresh: bool = False) -> list[dict]:
        task_status_types = ["done", "init", "paused", "aborted"] if task_status else []
        results = []

        def get_db_status(db: PlatformDatabaseModel) -> dict:
            row = {"name": db.name,
                   "platform": db.platform,
                   "path": str(db.db_path)}
            if db.exists():
                # print(db.name, db.content.file_size, int(db_utils.file_size(db)))
                running = db_utils.currently_open(db)
                size_changed = db.content.file_size != int(db_utils.file_size(db))
                if size_changed or running or force_refresh or not db.content.last_modified:
                    print(f"updating db stats for {db.name}")
                    self.update_db_base_stats(db)
                    if running:
                        row["name"] = f"[yellow]{row["name"]}[/yellow]"
                    else:  # updated
                        row["name"] = f"[blue]{row["name"]}[/blue]"

                db_content = db.content

                row.update({
                    "last mod": f"{datetime.fromtimestamp(db_content.last_modified):%Y-%m-%d %H:%M}",
                    "total": str(db_content.post_count),
                    "size": f"{int(db_content.file_size / (1024 * 1024))} Mb"})
                row.update({k: str(db_content.tasks_states.get(k, 0)) for k in task_status_types})
                # db.content.file_size = int(db_utils.file_size(db))

            else:
                row["path"] = f"[red]{row["path"]}[/red]"
            return row

        if databases:
            dbs = [self.get(d) for d in databases]
        else:
            dbs: list[PlatformDatabaseModel] = self.get_dbs()

        # Parallelize database status processing
        from concurrent.futures import ThreadPoolExecutor, as_completed

        with ThreadPoolExecutor(max_workers=min(len(dbs), 4)) as executor:
            # Submit all database status tasks
            future_to_db = {executor.submit(get_db_status, db): db for db in dbs}

            # Collect results as they complete
            for future in as_completed(future_to_db):
                db = future_to_db[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    # Handle individual database failures gracefully
                    error_result = {
                        "name": f"[red]{db.name}[/red]",
                        "platform": db.platform,
                        "path": f"[red]ERROR: {str(e)}[/red]"
                    }
                    results.append(error_result)

        results = sorted(results, key=lambda x: (x["platform"], x.get("last mod")))
        return results

    def update_db_base_stats(self, id_: int | str | PlatformDatabaseModel) -> PlatformDatabaseModel:
        db = self.get(id_) if not isinstance(id_, PlatformDatabaseModel) else id_
        db.update_base_stats()
        self.update_content(db)
        return db

    def rename(self, id_: int | str, new_name: str) -> PlatformDatabaseModel:

        def _rename(session, db_obj: DBPlatformDatabase):
            db_obj.name = new_name

        return self.edit(id_, _rename)

    def get_db_names(self) -> list[str]:
        return [db.name for db in self.get_dbs()]

    def set_alternative_path(self, db_name: str, alternative_path_name: str, alternative_path: Path):
        db = self.get(db_name)
        db.set_alternative_path(alternative_path_name, alternative_path.absolute())
        self.update_content(db)

    def copy_posts_metadata_content(self, db_name: str,
                                    alternative_name: str,
                                    field: str,
                                    direction: Literal["to_alternative", "to_main"],
                                    overwrite: bool = False):
        db = self.get(db_name)
        alt_dbs = db.content.alternative_paths or {}
        if alternative_name not in alt_dbs:
            raise ValueError(f"Database: {db_name} does not have the alternative: {alternative_name}")
        db_mgmt = db.get_mgmt()
        alt_mgmt = DatabaseManager.sqlite_db_from_path(alt_dbs[alternative_name])
        from big5_databases.databases.db_merge import copy_posts_metadata_content as _copy
        _copy(db_mgmt, alt_mgmt, field, direction == "to_alternative", overwrite)

    def update_content(self, db_model: PlatformDatabaseModel):
        def _update(session, db):
            # Validate content dict against MetaDatabaseContentModel before updating
            content_dict = db_model.content.model_dump()
            validated_content = MetaDatabaseContentModel.model_validate(content_dict)
            db.content = validated_content.model_dump()
            flag_modified(db, "content")

        # Use db_model.id or db_model.name as the identifier, not the whole model object
        identifier = db_model.id if db_model.id is not None else db_model.name
        self.edit(identifier, _update)

    def add_run_state(self, db_name: str, run_state: DatabaseRunState):
        db = self.get(db_name)
        if run_state.alt_db:
            if run_state.alt_db not in db.content.alternative_paths:
                raise ValueError(f"Database: {db_name} does not have the alternative: {run_state.alt_db}")
        db.add_run_state(run_state)
        self.update_content(db)

    def get_client_setup(self, db_name: str) -> "ClientSetup":
        """
        Get ClientSetup from database content if stored, otherwise build from database metadata.
        This enables simplified processor initialization with just a database name.
        """
        from .external import ClientSetup, ClientConfig, DBSetupConfig, SQliteConnection

        db = self.get(db_name)

        # Check if client_setup is stored in database content
        if db.content.client_setup:
            logger.debug(f"Using stored client_setup for database: {db_name}")
            return db.content.client_setup

        # Fallback: Build configuration from stored metadata
        logger.debug(f"Building client_setup from metadata for database: {db_name}")

        # Build database configuration from stored metadata
        db_setup = DBSetupConfig(
            name=db.name,
            db_connection=SQliteConnection(db_path=db.db_path),
            create=False,  # Database should already exist in MetaDatabase
            require_existing_parent_dir=True,
            tables=[]  # Will be set by platform manager
        )

        # Use stored config if available, otherwise use defaults
        client_config = db.content.config if db.content.config else ClientConfig(
            progress=True,  # Active by default
            request_delay=0,
            delay_randomize=0,
            ignore_initial_quota_halt=False
        )

        # Build complete client setup
        client_setup = ClientSetup(
            platform=db.platform,
            config=client_config,
            db=db_setup
        )

        return client_setup


def get_db_mgmt(config: Optional[DBConfig], metadatabase_path: Optional[Path],
                database_name: Optional[str]) -> DatabaseManager:
    """
    takes either a config or a meta-db-path and db-name
    """
    assert config or metadatabase_path and database_name, "Either database-config or metadatabase and database-name must be passed"
    if config:
        return DatabaseManager(DBConfig(
            db_connection=config,
            create=False
        ))
    else:
        return MetaDatabase(metadatabase_path).get(database_name).get_mgmt()
