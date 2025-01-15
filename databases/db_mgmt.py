from contextlib import contextmanager
from typing import Optional
from typing_extensions import deprecated

from sqlalchemy import create_engine, Engine, event
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database, drop_database

from databases.external import DBConfig, SQliteConnection
from databases.external import BASE_DATA_PATH
from databases.db_models import Base
from tools.project_logging import get_logger


class DatabaseManager:

    def __init__(self, config: DBConfig):
        self.config = config
        self.logger = get_logger(__file__)
        self.engine = self._create_engine()
        self.Session = sessionmaker(self.engine)
        self.init_database()

        if config.db_type == "sqlite":
            event.listen(self.engine, 'connect', self._sqlite_on_connect)


    def _create_engine(self) -> Engine:
        self.logger.debug(f"creating db engine with {self.config.connection_str}")
        return create_engine(self.config.connection_str)

    @classmethod
    def get_main_db_config(cls) -> "DBConfig":
        return DBConfig(
            db_connection=SQliteConnection(db_path=(BASE_DATA_PATH / "main.sqlite").as_posix()))

    @staticmethod
    def _sqlite_on_connect(dbapi_con, _):
        dbapi_con.execute('pragma foreign_keys=ON')

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
        return database_exists(self.config.connection_string)

    def init_database(self) -> None:
        """Initialize database, optionally resetting if configured."""
        if self.config.db_type == "postgres":
            self._create_postgres_db()
        else:  # sqlite
            if self.config.reset_db and database_exists(self.engine.url):
                if input(f"Delete existing database? (y/n): ").lower() == 'y':
                    drop_database(self.engine.url)
                else:
                    return

            if not database_exists(self.engine.url):
                create_database(self.engine.url)
                Base.metadata.create_all(self.engine)

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


class AsyncDatabaseManager(DatabaseManager):
    def __init__(self, config: DBConfig):
        super().__init__(config)
        self.async_engine = create_async_engine(config.connection_str)
        self.async_session = async_sessionmaker(self.async_engine)

    async def get_async_session(self) -> AsyncSession:
        return self.async_session()

# def get_orm_classes() -> dict[str,Base]:
#     return {
#         c.__tablename__: c for c in [DBCollectionTask, DBPost, DBComment]
#     }
