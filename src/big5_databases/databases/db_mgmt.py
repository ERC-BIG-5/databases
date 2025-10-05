import logging
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

from sqlalchemy import create_engine, Engine, event, exists, select
from sqlalchemy.dialects.sqlite.dml import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import delete, update
from sqlalchemy.sql.schema import Table
from sqlalchemy_utils import database_exists, create_database, drop_database
from tools.project_logging import get_logger

from . import db_analytics, db_operations
from .db_models import Base, DBPost, DBCollectionTask, CollectionResult
from .db_settings import SqliteSettings
from .db_stats import generate_db_stats
from .external import DBConfig, SQliteConnection, CollectionStatus, MetaDatabaseContentModel, ClientTaskConfig, \
    DatabaseBasestats
from .external import PostgresConnection
from .model_conversion import PlatformDatabaseModel, PostModel


class DatabaseManager:

    def __init__(self, config: DBConfig):
        self.config = config
        self.logger = get_logger(__file__)
        self.engine = self._create_engine()
        self.Session = sessionmaker(self.engine)
        self.init_database()
        self.metadata: Optional[PlatformDatabaseModel] = None  # through setter

        if self.config.db_type == "sqlite":
            event.listen(self.engine, 'connect', self._sqlite_on_connect)
        # todo, store here if its a regular: task, post; post-process-item, or meta-db
        #self.db_type = self.config.type

    def __repr__(self) -> str:
        return f"DBManager: {self.engine.url}"

    @staticmethod
    def sqlite_db_from_path(path: str | Path,
                            create: bool = False) -> "DatabaseManager":
        return DatabaseManager(DBConfig(db_connection=SQliteConnection(db_path=path),
                                        create=create, require_existing_parent_dir=True))

    def _create_engine(self) -> Engine:
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
        dbapi_con.execute('pragma foreign_keys=ON')
        dbapi_con.execute('pragma journal_mode=WAL')
        dbapi_con.execute('pragma synchronous=NORMAL')
        dbapi_con.execute('pragma auto_vacuum = FULL;')

    def _create_postgres_db(self) -> None:
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

    def db_exists(self):
        return database_exists(self.config.connection_str)

    def skip_confirmation_in_test(self, engine_url) -> bool:
        return self.config.test_mode and Path(engine_url.database).stem.endswith("test")

    def init_database(self) -> None:
        """Initialize database, optionally resetting if configured."""

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
                    tables_:dict[str,Table] = dict(Base.metadata.tables)
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
        """Provide a transactional scope around operations."""
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
        tables = list(Base.metadata.tables)[:]
        tables.remove("platform_databases")
        tables.remove("ppitem")
        return tables



    # File system utilities (private methods)
    def _file_size(self) -> int:
        """Get database file size in bytes."""
        if isinstance(self.config.db_connection, SQliteConnection):
            return self.config.db_connection.db_path.stat().st_size
        return 0

    def _file_modified(self) -> float:
        """Get database file modification timestamp."""
        if isinstance(self.config.db_connection, SQliteConnection):
            return self.config.db_connection.db_path.stat().st_mtime
        return 0.0

    def _currently_open(self) -> bool:
        """Check if database is currently open (has active WAL file)."""
        if isinstance(self.config.db_connection, SQliteConnection):
            wal_path = str(self.config.db_connection.db_path) + '-wal'
            return Path(wal_path).exists() and Path(wal_path).stat().st_size > 0
        return False


class AsyncDatabaseManager(DatabaseManager):
    def __init__(self, config: DBConfig):
        super().__init__(config)
        self.async_engine = create_async_engine(config.connection_str)
        self.async_session = async_sessionmaker(self.async_engine)

    async def get_async_session(self) -> AsyncSession:
        return self.async_session()
