import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Callable, Literal, TYPE_CHECKING

from sqlalchemy.exc import IntegrityError, NoResultFound
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.orm.session import Session
from tools.env_root import root
from tools.project_logging import get_logger

from big5_databases.databases.model_conversion import PlatformDatabaseModel
from .db_mgmt import DatabaseManager
from .db_models import DBPlatformDatabase
from .db_settings import SETTINGS
from .external import DBConfig, SQliteConnection, MetaDatabaseContentModel, PlatformDBConfig
from .external import DatabaseRunState

from .platform_db_mgmt import PlatformDB
if TYPE_CHECKING:
    from .platform_db_mgmt import PlatformDB

logger = get_logger(__file__)


class MetaDatabase:
    """
    Meta database manager for tracking and managing multiple platform databases.

    This class provides a centralized interface for managing multiple platform-specific
    databases, including registration, status tracking, configuration management,
    and database operations across different social media platforms.

    Parameters
    ----------
    db_path : Optional[Path], optional
        Path to the meta database file. If None, uses default path from settings
        or falls back to root()/"data/dbs/main.sqlite".
    create : bool, optional
        Whether to create the database if it doesn't exist, by default False.
    check_databases : bool, optional
        Whether to check all registered databases for existence on initialization,
        by default True.

    Attributes
    ----------
    db : DatabaseManager
        Database manager instance for the meta database operations.
    """

    def __init__(self, db_path: Optional[Path] = None, create: bool = False, check_databases: bool = True):
        """
        Initialize the MetaDatabase with optional path and configuration.

        Parameters
        ----------
        db_path : Optional[Path], optional
            Path to the meta database file, by default None.
        create : bool, optional
            Whether to create the database if it doesn't exist, by default False.
        check_databases : bool, optional
            Whether to verify all registered databases exist, by default True.
        """
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
        Check if all registered databases exist and return names of missing ones.

        Returns
        -------
        list[str]
            List of database names that are registered but whose files don't exist.
        """
        return [db.name for db in self.get_dbs() if not db.exists()]

    def get_dbs(self) -> list[PlatformDatabaseModel]:
        """
        Get all registered platform databases from the meta database.

        Returns
        -------
        list[PlatformDatabaseModel]
            List of all registered platform database models.
        """
        with self.db.get_session() as session:
            return [o.model() for o in session.query(DBPlatformDatabase).all()]

    def exists(self, id_: int | str | PlatformDatabaseModel) -> bool:
        """
        Check if a database exists in the meta database.

        Parameters
        ----------
        id_ : int, str, or PlatformDatabaseModel
            Database identifier (ID, name, or model object).

        Returns
        -------
        bool
            True if the database exists, False otherwise.
        """
        return self[id_] is not None

    def get_db_mgmt(self, id_: int | str | PlatformDatabaseModel) -> Optional[DatabaseManager]:
        """
        Get DatabaseManager for generic database operations (deprecated).

        Parameters
        ----------
        id_ : int, str, or PlatformDatabaseModel
            Database identifier (ID, name, or model object).

        Returns
        -------
        Optional[DatabaseManager]
            DatabaseManager instance or None if not found.

        Notes
        -----
        This method is deprecated. Use get_platform_db() for platform-specific operations.
        """
        db = self.get(id_)
        dbm = db.get_mgmt(self)
        return dbm

    def get_platform_db(self, id_: int | str | PlatformDatabaseModel,
                        table_type: Literal["posts", "process"] = "posts") -> "PlatformDB":
        """
        Get proper PlatformDB instance with platform context.

        Parameters
        ----------
        id_ : int, str, or PlatformDatabaseModel
            Database identifier (ID, name, or model object).
        table_type : Literal["posts", "process"], optional
            Type of tables to use, by default "posts".

        Returns
        -------
        PlatformDB
            Platform-specific database manager instance.
        """
        db = self.get(id_)

        # todo: bring back
        # if not self.exists():
        #     raise ValueError(f"Could not load database {self.db_path} from meta-database. Database does not exist")

        config = PlatformDBConfig(
            platform=db.platform,
            db_connection=SQliteConnection(db_path=db.db_path),
            table_type=table_type,
            create=False,
            require_existing_parent_dir=True
        )

        platform_db = PlatformDB(config)
        platform_db.metadata = db  # Set metadata reference
        return platform_db

    def __getitem__(self, id_: int | str | PlatformDatabaseModel) -> Optional[PlatformDatabaseModel]:
        """
        Get database model by ID, name, or model object (dict-like access).

        Parameters
        ----------
        id_ : int, str, or PlatformDatabaseModel
            Database identifier (ID, name, or model object).

        Returns
        -------
        Optional[PlatformDatabaseModel]
            Database model or None if not found.
        """
        return self.edit(id_)

    def get(self, id_: int | str | PlatformDatabaseModel) -> PlatformDatabaseModel:
        """
        Get database model by ID, name, or model object, raising error if not found.

        Parameters
        ----------
        id_ : int, str, or PlatformDatabaseModel
            Database identifier (ID, name, or model object).

        Returns
        -------
        PlatformDatabaseModel
            Database model.

        Raises
        ------
        ValueError
            If the database does not exist.
        """
        if isinstance(id_, PlatformDatabaseModel):
            return id_
        db = self[id_]
        if not db:
            raise ValueError(f"Database : {id_} does not exist")
        return db

    def has(self, id_: int | str | PlatformDatabaseModel) -> bool:
        """
        Check if a database is registered in the meta database.

        Parameters
        ----------
        id_ : int, str, or PlatformDatabaseModel
            Database identifier (ID, name, or model object).

        Returns
        -------
        bool
            True if the database is registered, False otherwise.
        """
        with self.db.get_session() as session:
            return self._get_obj(session, id_) is not None

    def _get_obj(self, session, id_: int | str) -> Optional[DBPlatformDatabase]:
        """
        Internal method to get database object from session.

        Parameters
        ----------
        session : Session
            Database session.
        id_ : int, str, or PlatformDatabaseModel
            Database identifier.

        Returns
        -------
        Optional[DBPlatformDatabase]
            Database object or None if not found.
        """
        if isinstance(id_, PlatformDatabaseModel):
            id_ = id_.id
        if isinstance(id_, int):
            db_obj = session.query(DBPlatformDatabase).where(DBPlatformDatabase.id == id_).one_or_none()
        else:
            db_obj = session.query(DBPlatformDatabase).where(DBPlatformDatabase.name == id_).one_or_none()
        return None

    def get_obj(self, session, id_: int | str) -> DBPlatformDatabase:
        """
        Get database object from session, raising error if not found.

        Parameters
        ----------
        session : Session
            Database session.
        id_ : int, str, or PlatformDatabaseModel
            Database identifier.

        Returns
        -------
        DBPlatformDatabase
            Database object.

        Raises
        ------
        ValueError
            If the database is not found, with suggestions for similar names.
        """
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
        """
        Edit a database entry using a provided function.

        Parameters
        ----------
        id_ : int, str, or PlatformDatabaseModel
            Database identifier.
        func : Optional[Callable[[Session, DBPlatformDatabase], None]], optional
            Function to apply to the database object, by default None.
        model : Optional[bool], optional
            Whether to return the model object, by default True.

        Returns
        -------
        Optional[PlatformDatabaseModel]
            Updated database model.
        """
        with self.db.get_session() as session:
            db_obj = self.get_obj(session, id_)
            if func is None:
                def func_(session_, obj_):
                    return None

                func = func_
            func(session, db_obj)
            return db_obj.model()

    def set_db_path(self, id_: int | str, new_path: Path):
        """
        Set a new database file path for a registered database.

        Parameters
        ----------
        id_ : int or str
            Database identifier (ID or name).
        new_path : Path
            New path to the database file.

        Raises
        ------
        ValueError
            If the new path doesn't exist.
        """

        # check if path exists, either if its absolute or relative to the default path
        if new_path.is_absolute() and not new_path.exists() and not (
                SETTINGS.default_sqlite_dbs_base_path / new_path).exists():
            raise ValueError(f"No database at location: {new_path}")

        def _set_db_path(session_: Session, db_obj: DBPlatformDatabase):
            db_obj.db_path = str(new_path)

        self.edit(id_, _set_db_path)

    def add_db(self, db: PlatformDatabaseModel, client_setup: Optional["ClientSetup"] = None) -> bool:
        """
        Add a new database to the meta database registry.

        Parameters
        ----------
        db : PlatformDatabaseModel
            Database model to add.
        client_setup : Optional[ClientSetup], optional
            Client setup configuration to store with the database, by default None.

        Returns
        -------
        bool
            True if successfully added, False if failed due to integrity error.
        """
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
            # todo, eventually bring this back...
            # self.update_db_base_stats(db.name)
        except IntegrityError as e:
            logger.error(f"Could not add database {db.name} to meta-database: {e.orig}")
            session.rollback()
            return False
        return True

    def delete(self, id_: int | str):
        """
        Delete a database from the registry and optionally from filesystem.

        Parameters
        ----------
        id_ : int or str
            Database identifier (ID or name).

        Notes
        -----
        This method prompts the user for confirmation before deleting the
        actual database file. It also handles alternative database paths.
        """
        # this is more robust cuz it also removes broken dbs that dont validate to the model
        full_path = None
        with self.db.get_session() as session:
            db = self.get_obj(session, id_)
            if not db:
                print(f"database not found: {id_}")
                return
            alt_paths = db.content["alternative_paths"]
            full_path = db.full_path
            session.delete(db)

        delete_file = input("Delete the file {full_path}: [y] or mark?")

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
        """
        Remove all database entries whose files no longer exist.

        Parameters
        ----------
        simulate : bool, optional
            If True, only print what would be deleted without actually deleting,
            by default False.
        """
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
        """
        Get comprehensive status information for databases.

        Parameters
        ----------
        databases : Optional[list[str]], optional
            List of specific database names to check. If None, checks all databases,
            by default None.
        task_status : bool, optional
            Whether to include task status counts, by default True.
        force_refresh : bool, optional
            Whether to force refresh of database statistics, by default False.

        Returns
        -------
        list[dict]
            List of dictionaries containing status information for each database.

        Notes
        -----
        Uses parallel processing to improve performance when checking multiple databases.
        """
        task_status_types = ["done", "init", "paused", "aborted"] if task_status else []
        results = []

        def get_db_status(db: PlatformDatabaseModel) -> dict:
            row = {"name": db.name,
                   "platform": db.platform,
                   "path": str(db.db_path)}
            if db.exists():
                # Use PlatformDB directly (inherits from DatabaseManager)
                platform_db = self.get_platform_db(db)
                #running = platform_db._currently_open()
                running = False

                size_changed = db.content.file_size != int(platform_db._file_size())
                if size_changed or running or force_refresh or not db.content.last_modified:
                    print(f"updating db stats for {db.name}")
                    # FIX: Use existing platform_db to avoid recursive call cycle
                    base_stats = platform_db.calc_db_content()
                    db.content.add_basestats(base_stats)
                    self.update_content(db)
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
                    import traceback
                    print(traceback.format_exc())
                    results.append(error_result)

        results = sorted(results, key=lambda x: (x["platform"], x.get("last mod")))
        return results

    def update_db_base_stats(self, id_: int | str | PlatformDatabaseModel) -> PlatformDatabaseModel:
        """
        Update basic statistics for a database.

        Parameters
        ----------
        id_ : int, str, or PlatformDatabaseModel
            Database identifier.

        Returns
        -------
        PlatformDatabaseModel
            Updated database model with refreshed statistics.
        """
        db = self.get(id_) if not isinstance(id_, PlatformDatabaseModel) else id_
        db.update_base_stats()
        self.update_content(db)
        return db

    def rename(self, id_: int | str, new_name: str) -> PlatformDatabaseModel:
        """
        Rename a database in the registry.

        Parameters
        ----------
        id_ : int or str
            Database identifier (ID or current name).
        new_name : str
            New name for the database.

        Returns
        -------
        PlatformDatabaseModel
            Updated database model with new name.
        """

        def _rename(session, db_obj: DBPlatformDatabase):
            db_obj.name = new_name

        return self.edit(id_, _rename)

    def get_db_names(self) -> list[str]:
        """
        Get names of all registered databases.

        Returns
        -------
        list[str]
            List of database names.
        """
        return [db.name for db in self.get_dbs()]

    def set_alternative_path(self, db_name: str, alternative_path_name: str, alternative_path: Path):
        """
        Set an alternative path for a database.

        Parameters
        ----------
        db_name : str
            Name of the database.
        alternative_path_name : str
            Name/identifier for the alternative path.
        alternative_path : Path
            Path to the alternative database file.
        """
        db = self.get(db_name)
        db.set_alternative_path(alternative_path_name, alternative_path.absolute())
        self.update_content(db)

    def copy_posts_metadata_content(self, db_name: str,
                                    alternative_name: str,
                                    field: str,
                                    direction: Literal["to_alternative", "to_main"],
                                    overwrite: bool = False):
        """
        Copy posts metadata content between main and alternative databases.

        Parameters
        ----------
        db_name : str
            Name of the main database.
        alternative_name : str
            Name of the alternative database.
        field : str
            Metadata field to copy.
        direction : Literal["to_alternative", "to_main"]
            Direction of the copy operation.
        overwrite : bool, optional
            Whether to overwrite existing metadata, by default False.

        Raises
        ------
        ValueError
            If the alternative database is not registered.
        """
        db = self.get(db_name)
        alt_dbs = db.content.alternative_paths or {}
        if alternative_name not in alt_dbs:
            raise ValueError(f"Database: {db_name} does not have the alternative: {alternative_name}")
        db_mgmt = db.get_mgmt()
        alt_mgmt = DatabaseManager.sqlite_db_from_path(alt_dbs[alternative_name])
        from big5_databases.databases.db_merge import copy_posts_metadata_content as _copy
        _copy(db_mgmt, alt_mgmt, field, direction == "to_alternative", overwrite)

    def update_content(self, db_model: PlatformDatabaseModel):
        """
        Update the content field of a database in the registry.

        Parameters
        ----------
        db_model : PlatformDatabaseModel
            Database model with updated content to save.
        """
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
        """
        Add a run state record to a database.

        Parameters
        ----------
        db_name : str
            Name of the database.
        run_state : DatabaseRunState
            Run state information to add.

        Raises
        ------
        ValueError
            If the alternative database specified in run_state is not registered.
        """
        db = self.get(db_name)
        if run_state.alt_db:
            if run_state.alt_db not in db.content.alternative_paths:
                raise ValueError(f"Database: {db_name} does not have the alternative: {run_state.alt_db}")
        db.add_run_state(run_state)
        self.update_content(db)

    def get_client_setup(self, db_name: str) -> "ClientSetup":
        """
        Get ClientSetup from database content or build from metadata.

        Parameters
        ----------
        db_name : str
            Name of the database.

        Returns
        -------
        ClientSetup
            Client setup configuration for the database.

        Notes
        -----
        If client_setup is stored in the database content, it will be returned directly.
        Otherwise, a new ClientSetup is built from the stored database metadata.
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
    Get DatabaseManager - deprecated, use get_platform_db for platform-specific operations.

    Parameters
    ----------
    config : Optional[DBConfig]
        Database configuration object. If provided, creates DatabaseManager directly.
    metadatabase_path : Optional[Path]
        Path to the meta database file.
    database_name : Optional[str]
        Name of the database in the meta database.

    Returns
    -------
    DatabaseManager
        Database manager instance.

    Notes
    -----
    This function is deprecated. Use get_platform_db() for platform-specific operations.
    Either config must be provided, or both metadatabase_path and database_name.

    Raises
    ------
    AssertionError
        If neither config nor both metadatabase_path and database_name are provided.
    """
    logger.warning("get_db_mgmt() is deprecated. Use get_platform_db() for platform-specific operations.")
    assert config or metadatabase_path and database_name, "Either database-config or metadatabase and database-name must be passed"
    if config:
        return DatabaseManager(DBConfig(
            db_connection=config,
            create=False
        ))
    else:
        return MetaDatabase(metadatabase_path).get(database_name).get_mgmt()


def get_platform_db(metadatabase_path: Path, database_name: str,
                    table_type: Literal["posts", "process"] = "posts") -> "PlatformDB":
    """
    Get proper PlatformDB instance with platform context from meta database.

    Parameters
    ----------
    metadatabase_path : Path
        Path to the meta database file.
    database_name : str
        Name of the platform database to retrieve.
    table_type : Literal["posts", "process"], optional
        Type of tables to use ("posts" or "process"), by default "posts".

    Returns
    -------
    PlatformDB
        Platform database instance with proper platform context.
    """
    meta_db = MetaDatabase(metadatabase_path)
    return meta_db.get_platform_db(database_name, table_type=table_type)


def get_post_process_db(database_path: Path):
    """
    Get a DatabaseManager for post-processing database operations.

    Parameters
    ----------
    database_path : Path
        Path to the post-processing database file.

    Returns
    -------
    DatabaseManager
        Database manager configured for post-processing items table.

    Notes
    -----
    This is a temporary function that provides direct path access to
    post-processing databases. It creates a DatabaseManager with only
    the "ppitem" table configured.
    """
    return DatabaseManager(DBConfig(create=False,
                                    tables=["ppitem"],
                                    db_connection=SQliteConnection(db_path=database_path)))
