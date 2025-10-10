import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

from sqlalchemy import create_engine, Engine, event
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.schema import Table
from sqlalchemy_utils import database_exists, create_database, drop_database
from tools.project_logging import get_logger

from .db_models import Base, DBPost
from .external import DBConfig, SQliteConnection
from .external import PostgresConnection
from .model_conversion import PlatformDatabaseModel


class DatabaseManager:
    """
    Database management class for handling SQLite and PostgreSQL database operations.

    This class provides a unified interface for database operations including
    connection management, table creation, session handling, and database utilities.

    Parameters
    ----------
    config : DBConfig
        Database configuration object containing connection details and settings.

    Attributes
    ----------
    config : DBConfig
        Database configuration object.
    logger : logging.Logger
        Logger instance for database operations.
    engine : sqlalchemy.Engine
        SQLAlchemy engine for database connections.
    Session : sqlalchemy.orm.sessionmaker
        Session factory for creating database sessions.
    metadata : Optional[PlatformDatabaseModel]
        Optional metadata for platform database model.

    """

    def __init__(self, config: DBConfig):
        """
        Initialize the DatabaseManager with configuration.

        Parameters
        ----------
        config : DBConfig
            Database configuration object containing connection details,
            database type, and other settings.

        Notes
        -----
        This method creates the database engine, session factory, initializes
        the database, and sets up SQLite-specific event listeners if applicable.
        """
        self.config = config
        self.logger = get_logger(__file__)
        self.engine = self._create_engine()
        self.Session = sessionmaker(self.engine)
        self.init_database()
        self.metadata: Optional[PlatformDatabaseModel] = None  # through setter

        if self.config.db_type == "sqlite":
            event.listen(self.engine, 'connect', self._sqlite_on_connect)
        # todo, store here if its a regular: task, post; post-process-item, or meta-db
        # self.db_type = self.config.type

    def __repr__(self) -> str:
        """
        Return string representation of the DatabaseManager.

        Returns
        -------
        str
            String containing the class name and database URL.
        """
        return f"DBManager: {self.engine.url}"

    @staticmethod
    def sqlite_db_from_path(path: str | Path,
                            create: bool = False) -> "DatabaseManager":
        """
        Create a DatabaseManager instance from a SQLite database path.

        Parameters
        ----------
        path : str or Path
            Path to the SQLite database file.
        create : bool, optional
            Whether to create the database if it doesn't exist, by default False.

        Returns
        -------
        DatabaseManager
            New DatabaseManager instance configured for the SQLite database.

        """
        return DatabaseManager(DBConfig(db_connection=SQliteConnection(db_path=path),
                                        create=create, require_existing_parent_dir=True))

    def _create_engine(self) -> Engine:
        """
        Create and configure the SQLAlchemy engine.

        Returns
        -------
        sqlalchemy.Engine
            Configured SQLAlchemy engine for database connections.

        Notes
        -----
        Creates an engine using the connection string from the configuration.
        Additional connection arguments can be added based on database type.
        """
        self.logger.debug(f"creating db engine with {self.config.connection_str}")
        connect_args = {}
        # if self.config.db_type == "sqlite":
        #     # Add timeout and isolation level settings
        #     connect_args.update({
        #         'timeout': 30,  # seconds
        #         'isolation_level': 'IMMEDIATE'  # this helps with write conflicts
        #     })
        return create_engine(
            self.config.connection_str,
            connect_args=connect_args
        )

    @staticmethod
    def _sqlite_on_connect(dbapi_con, _):
        """
        Configure SQLite connection with optimal settings.

        Parameters
        ----------
        dbapi_con : sqlite3.Connection
            SQLite database connection object.
        _ : Any
            Unused connection record parameter.

        Notes
        -----
        This method is called automatically when a new SQLite connection is
        established. It enables foreign keys, sets WAL mode for better
        concurrency, normal synchronization for performance, and full
        auto-vacuum for space management.
        """
        dbapi_con.execute('pragma foreign_keys=ON')
        dbapi_con.execute('pragma journal_mode=WAL')
        dbapi_con.execute('pragma synchronous=NORMAL')
        dbapi_con.execute('pragma auto_vacuum = FULL;')

    def _create_postgres_db(self) -> None:
        """
        Create and initialize a PostgreSQL database.

        Notes
        -----
        Checks if the database exists and handles database creation or
        reset based on configuration. If reset_db is True and database
        exists, prompts user for confirmation before dropping.
        Creates all tables defined in Base.metadata after database creation.

        Raises
        ------
        Various SQLAlchemy exceptions
            If database operations fail.
        """
        if database_exists(self.config.connection_str):
            if self.config.reset_db:
                if input(f"Database {self.config.name} exists. Drop it? (y/n): ").lower() == 'y':
                    drop_database(self.config.connection_str)
                else:
                    return
            else:
                return

        create_database(self.config.connection_str)
        Base.metadata.create_all(self.engine)

    def db_exists(self) -> bool:
        """
        Check if the database exists.

        Returns
        -------
        bool
            True if the database exists, False otherwise.

        Notes
        -----
        Uses sqlalchemy_utils.database_exists to check database existence
        for both SQLite and PostgreSQL databases.
        """
        return database_exists(self.config.connection_str)

    def skip_confirmation_in_test(self, engine_url) -> bool:
        """
        Determine if confirmation prompts should be skipped in test mode.

        Parameters
        ----------
        engine_url : sqlalchemy.engine.url.URL
            SQLAlchemy engine URL object.

        Returns
        -------
        bool
            True if running in test mode and database name ends with "test",
            False otherwise.

        Notes
        -----
        This method helps automate database operations during testing by
        skipping user confirmation prompts for test databases.
        """
        return self.config.test_mode and Path(engine_url.database).stem.endswith("test")

    def init_database(self) -> None:
        """
        Initialize database, optionally resetting if configured.

        Notes
        -----
        This method handles database initialization for both SQLite and
        PostgreSQL databases. For SQLite, it:
        - Checks if database exists when create=False
        - Handles database reset if configured
        - Creates database file and directory structure
        - Creates specified tables or all platform tables

        For PostgreSQL, it delegates to _create_postgres_db().

        Raises
        ------
        ValueError
            If database doesn't exist and create=False, or if parent
            directory doesn't exist when require_existing_parent_dir=True.
        """

        if self.config.db_type == "sqlite":
            if not self.config.create and not database_exists(self.config.db_connection.connection_str):
                raise ValueError(f"Database {self.config.connection_str} does not exist")

            if self.config.reset_db and database_exists(self.engine.url):
                if self.skip_confirmation_in_test(self.engine.url):
                    drop_database(self.engine.url)
                else:
                    if input(f"Delete existing database ({self.config.db_connection.db_path})? (y/n): ").lower() == 'y':
                        drop_database(self.engine.url)
                    else:
                        return

            if not database_exists(self.engine.url):
                db_path = Path(self.config.db_connection.db_path)
                if self.config.require_existing_parent_dir:
                    if not db_path.parent.exists():
                        raise ValueError(f"Parent directory {db_path.parent} does not exist")
                db_path.parent.mkdir(parents=True, exist_ok=True)
                # create an empty db file
                create_database(self.engine.url)
                if self.config.tables:
                    # Base.metadata.create_all(self.engine)
                    md = Base.metadata.tables
                    # this could crash, if we pass a wrong table...
                    tables = [md[table] for table in self.config.tables]
                    self.logger.debug(f"Creating database tables: {tables}")
                    Base.metadata.create_all(self.engine, tables=tables)
                else:
                    # no "platform_databases" for normal tables
                    tables_: dict[str, Table] = dict(Base.metadata.tables)
                    for table in ["platform_databases", "ppitem"]:
                        if table in tables_:
                            del tables_[table]
                    Base.metadata.create_all(self.engine, tables=list(tables_.values()))
        else:
            PostgresConnection.model_validate(self.config)
            self._create_postgres_db()
            return

    @contextmanager
    def get_session(self):
        """
        Provide a transactional scope around database operations.

        Yields
        ------
        sqlalchemy.orm.Session
            Database session for performing operations.

        Notes
        -----
        This context manager ensures proper transaction handling:
        - Commits the transaction if no exceptions occur
        - Rolls back the transaction if any exception is raised
        - Always closes the session in the finally block

        Examples
        --------
        >>> with db_manager.get_session() as session:
        ...     result = session.query(DBPost).all()
        ...     session.add(new_post)
        """
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    @staticmethod
    def platform_tables() -> list[str]:
        """
        Get list of platform-specific table names.

        Returns
        -------
        list[str]
            List of table names excluding 'platform_databases' and 'ppitem'.

        Notes
        -----
        This method returns all tables defined in Base.metadata except
        for meta-tables that are not part of regular platform operations.
        Useful for selective table operations and migrations.
        """
        tables = list(Base.metadata.tables)[:]
        tables.remove("platform_databases")
        tables.remove("ppitem")
        return tables

    # File system utilities (private methods)
    def _file_size(self) -> int:
        """
        Get database file size in bytes.

        Returns
        -------
        int
            Size of the database file in bytes, or 0 if not a SQLite database.

        Notes
        -----
        This method only works for SQLite databases. For other database
        types, it returns 0 as file size is not directly accessible.
        """
        if isinstance(self.config.db_connection, SQliteConnection):
            return self.config.db_connection.db_path.stat().st_size
        return 0

    def _file_modified(self) -> float:
        """
        Get database file modification timestamp.

        Returns
        -------
        float
            Unix timestamp of last file modification, or 0.0 if not SQLite.

        Notes
        -----
        This method only works for SQLite databases. For other database
        types, it returns 0.0 as file modification time is not applicable.
        """
        if isinstance(self.config.db_connection, SQliteConnection):
            return self.config.db_connection.db_path.stat().st_mtime
        return 0.0

    def _currently_open(self) -> bool:
        """
        Check if database is currently open (has active WAL file).

        Returns
        -------
        bool
            True if database has an active WAL file with content,
            False otherwise or if not a SQLite database.

        Notes
        -----
        This method checks for the existence and non-zero size of the
        Write-Ahead Logging (WAL) file, which indicates active database
        connections. Only applicable to SQLite databases in WAL mode.
        """
        if isinstance(self.config.db_connection, SQliteConnection):
            wal_path = str(self.config.db_connection.db_path) + '-wal'
            return Path(wal_path).exists() and Path(wal_path).stat().st_size > 0
        return False


class AsyncDatabaseManager(DatabaseManager):
    """
    Asynchronous database management class extending DatabaseManager.

    This class provides asynchronous database operations while inheriting
    all synchronous functionality from DatabaseManager. It adds async
    engine and session management for non-blocking database operations.

    Parameters
    ----------
    config : DBConfig
        Database configuration object containing connection details and settings.

    Attributes
    ----------
    async_engine : sqlalchemy.ext.asyncio.AsyncEngine
        Asynchronous SQLAlchemy engine for non-blocking database connections.
    async_session : sqlalchemy.ext.asyncio.async_sessionmaker
        Factory for creating asynchronous database sessions.

    Notes
    -----
    This class inherits all attributes and methods from DatabaseManager,
    providing both synchronous and asynchronous database access patterns.
    """

    def __init__(self, config: DBConfig):
        """
        Initialize the AsyncDatabaseManager with configuration.

        Parameters
        ----------
        config : DBConfig
            Database configuration object containing connection details,
            database type, and other settings.

        Notes
        -----
        Calls parent DatabaseManager.__init__() to set up synchronous
        components, then creates additional async engine and session factory
        for asynchronous operations.
        """
        super().__init__(config)
        self.async_engine = create_async_engine(config.connection_str)
        self.async_session = async_sessionmaker(self.async_engine)

    async def get_async_session(self) -> AsyncSession:
        """
        Create and return an asynchronous database session.

        Returns
        -------
        sqlalchemy.ext.asyncio.AsyncSession
            Asynchronous database session for non-blocking operations.

        Notes
        -----
        Unlike the synchronous get_session() context manager, this method
        returns a session that must be manually managed. Consider using
        it within an async context manager for proper resource cleanup.
        """
        return self.async_session()
