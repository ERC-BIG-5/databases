from contextlib import contextmanager
from pathlib import Path
from typing import Optional

from sqlalchemy import create_engine, Engine, event
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database, drop_database
from tools.project_logging import get_logger

from . import db_utils
from .db_models import Base, DBPost, DBCollectionTask
from .db_stats import generate_db_stats
from .db_utils import filter_posts_with_existing_post_ids
from .external import DBConfig, SQliteConnection, CollectionStatus, MetaDatabaseContentModel
from .external import PostgresConnection
from .model_conversion import PlatformDatabaseModel


class DatabaseManager:

    def __init__(self, config: DBConfig, db_meta: Optional[PlatformDatabaseModel] = None):
        self.config = config
        self.logger = get_logger(__file__)
        self.engine = self._create_engine()
        self.Session = sessionmaker(self.engine)
        self.init_database()
        self.metadata: Optional[PlatformDatabaseModel] = None  # through setter

        if self.config.db_type == "sqlite":
            event.listen(self.engine, 'connect', self._sqlite_on_connect)

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

    def skip_confirmation_in_test(self, engine_url) -> True:
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
                db_path = self.config.db_connection.db_path
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
                    self.logger.info(f"Creating database tables: {tables}")
                    Base.metadata.create_all(self.engine, tables=tables)
                else:
                    # no "platform_databases" for normal tables
                    tables = dict(Base.metadata.tables)
                    if "platform_databases" in tables:
                        del tables["platform_databases"]
                    Base.metadata.create_all(self.engine, tables=tables.values())
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

    def safe_submit_posts(self, posts: list[DBPost]) -> list[DBPost]:
        submit_posts = posts
        while True:
            try:
                self.submit_posts(submit_posts)
                return submit_posts
            except IntegrityError as e:
                with self.get_session() as session:
                    submit_posts = filter_posts_with_existing_post_ids(posts, session)
                    return submit_posts
            except Exception as e:
                self.logger.error(f"Error submitting posts: {str(e)}")
                return []

    def submit_posts(self, posts: list[DBPost]):
        with self.get_session() as session:
            session.add_all(posts)
            session.commit()

    def update_task(self, task_id: int, status: str, found_items: int, added_items: int, duration: int):
        with self.get_session() as session:
            task = session.query(DBCollectionTask).get(task_id)
            task.status = status
            task.found_items = found_items
            task.added_items = added_items
            task.collection_duration = int(duration * 1000)
            session.commit()

    @staticmethod
    def platform_tables() -> list[str]:
        tables = list(Base.metadata.tables)[:]
        tables.remove("platform_databases")
        return tables

    def reset_collection_task_states(self,
                                     states: list[CollectionStatus] = (CollectionStatus.RUNNING,
                                                                       CollectionStatus.ABORTED)) -> int:

        with self.get_session() as session:
            tasks = session.query(DBCollectionTask).filter(
                DBCollectionTask.status.in_(list(states))
            ).all()

            c = 0
            for t in tasks:
                t.status = CollectionStatus.INIT
                c += 1
        return c

    def calc_db_content(self) -> MetaDatabaseContentModel:
        return MetaDatabaseContentModel(
            tasks_states=db_utils.count_states(self),
            post_count=db_utils.count_posts(db=self),
            file_size=db_utils.file_size(self),
            last_modified=db_utils.file_modified(self),
            stats=generate_db_stats(self))


class AsyncDatabaseManager(DatabaseManager):
    def __init__(self, config: DBConfig):
        super().__init__(config)
        self.async_engine = create_async_engine(config.connection_str)
        self.async_session = async_sessionmaker(self.async_engine)

    async def get_async_session(self) -> AsyncSession:
        return self.async_session()
