from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import create_engine, Engine, event
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database, drop_database

from databases.db_models import Base, DBPost, DBCollectionTask
from databases.db_utils import filter_posts_with_existing_post_ids
from databases.external import BASE_DATA_PATH, PostgresConnection
from databases.external import DBConfig, SQliteConnection
from tools.project_logging import get_logger


class DatabaseManager:

    def __init__(self, config: DBConfig):
        self.config = config
        self.logger = get_logger(__file__)
        self.engine = self._create_engine()
        self.Session = sessionmaker(self.engine)
        self.init_database()

        if self.config.db_type == "sqlite":
            event.listen(self.engine, 'connect', self._sqlite_on_connect)

    @staticmethod
    def sqlite_db_from_path(path: Path,
                            create: bool = False) -> "DatabaseManager":
        return DatabaseManager(DBConfig(db_connection=SQliteConnection(db_path=path),
                                        create=create))

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

    @classmethod
    def get_main_db_config(cls) -> "DBConfig":
        return DBConfig(
            db_connection=SQliteConnection(db_path=(BASE_DATA_PATH / "main.sqlite").as_posix()))

    @staticmethod
    def _sqlite_on_connect(dbapi_con, _):
        dbapi_con.execute('pragma foreign_keys=ON')
        dbapi_con.execute('pragma journal_mode=WAL')
        dbapi_con.execute('pragma synchronous=NORMAL')

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

    def skip_confirmation_in_test(self, engine_url) -> True:
        return self.config.test_mode and Path(engine_url.database).stem.endswith("test")

    def init_database(self) -> None:
        """Initialize database, optionally resetting if configured."""

        if self.config.db_type == "sqlite":
            if not self.config.create and not database_exists(self.config.connection_str):
                raise ValueError(f"Database {self.config.connection_str} does not exist")

            if self.config.reset_db and database_exists(self.engine.url):
                if self.skip_confirmation_in_test(self.engine.url):
                    drop_database(self.engine.url)
                else:
                    if input(f"Delete existing database? (y/n): ").lower() == 'y':
                        drop_database(self.engine.url)
                    else:
                        return

            if not database_exists(self.engine.url):
                db_path = self.config.db_connection.db_path
                if self.config.require_existing_parent_dir:
                    if not db_path.parent.exists():
                        raise ValueError(f"Parent directory {db_path.parent} does not exist")
                db_path.parent.mkdir(parents=True, exist_ok=True)
                create_database(self.engine.url)
                if not self.config.tables:
                    Base.metadata.create_all(self.engine)
                else:
                    md = Base.metadata.tables
                    tables = [md[table] for table in self.config.tables]
                    Base.metadata.create_all(self.engine, tables=tables)

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
                submit_posts = filter_posts_with_existing_post_ids(posts, self)
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

    def count_states(self) -> dict[str, int]:
        """
        Count DBCollectionTask grouped by status
        todo, maybe util or stats?
        :return:
        """
        with self.get_session() as session:
            query = (
                session.query(
                    DBCollectionTask.status,
                    func.count(DBCollectionTask.status).label('count')
                )
                .group_by(DBCollectionTask.status)
            )

            results = query.all()
            return {enum_type.name.lower(): count for enum_type, count in results}



class AsyncDatabaseManager(DatabaseManager):
    def __init__(self, config: DBConfig):
        super().__init__(config)
        self.async_engine = create_async_engine(config.connection_str)
        self.async_session = async_sessionmaker(self.async_engine)

    async def get_async_session(self) -> AsyncSession:
        return self.async_session()
