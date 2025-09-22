import logging
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, TypedDict

from sqlalchemy import create_engine, Engine, event, exists, select
from sqlalchemy.dialects.sqlite.dml import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import delete, update
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
                    self.logger.debug(f"Creating database tables: {tables}")
                    Base.metadata.create_all(self.engine, tables=tables)
                else:
                    # no "platform_databases" for normal tables
                    tables = dict(Base.metadata.tables)
                    for table in ["platform_databases", "ppitem"]:
                        if table in tables:
                            del tables[table]
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
                self._submit_posts(submit_posts)
                return submit_posts
            except IntegrityError as e:
                with self.get_session() as session:
                    submit_posts = db_operations.filter_posts_with_existing_post_ids(posts, session)
                    return submit_posts
            except Exception as e:
                self.logger.error(f"Error submitting posts: {str(e)}")
                return []

    def _submit_posts(self, posts: list[DBPost]):
        with self.get_session() as session:
            # stmt = insert(DBPost).values()
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
        tables.remove("ppitem")
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

    def calc_db_content(self) -> DatabaseBasestats:
        return DatabaseBasestats(
            tasks_states=db_operations.count_states(self),
            post_count=db_analytics.count_posts(db=self),
            file_size=self._file_size(),
            last_modified=self._file_modified())


    @classmethod
    def get_platform_default_db(cls, platform: str) -> "DatabaseManager":
        """Create a DatabaseManager for the default platform database."""
        db_path = SqliteSettings().default_sqlite_dbs_base_path / f"{platform}.sqlite"
        return cls.sqlite_db_from_path(db_path)

    @classmethod
    def platform_db_from_path(cls, platform: str, path: str | Path, create: bool = False) -> "DatabaseManager":
        """Create a platform-specific DatabaseManager from a path."""
        return cls.sqlite_db_from_path(path, create)

    # Task management methods
    def check_task_name_exists(self, task_name: str) -> bool:
        """Check if a task name already exists in the database."""
        with self.get_session() as session:
            return session.query(exists().where(DBCollectionTask.task_name == task_name)).scalar()

    def check_task_names_exists(self, task_names: list[str]) -> list[str]:
        """Check which task names from the list already exist in the database."""
        with self.get_session() as session:
            existing_tasks = session.scalars(
                select(DBCollectionTask.task_name).where(DBCollectionTask.task_name.in_(task_names))).all()
            return existing_tasks

    def delete_tasks(self, task_names_keep_info: list[tuple[str, bool]]) -> None:
        """
        Delete tasks, optionally keeping their posts.
        
        :param task_names_keep_info: List of tuples (task_name, keep_posts)
        """
        with self.get_session() as session:
            keep_posts_of_tasks = [ti[0] for ti in task_names_keep_info if ti[1]]

            # Unlink posts from tasks that should keep their posts
            if keep_posts_of_tasks:
                keep_posts_ids = session.query(DBCollectionTask.id).where(
                    DBCollectionTask.task_name.in_(keep_posts_of_tasks)).all()
                keep_posts_ids = [k[0] for k in keep_posts_ids]
                session.execute(
                    update(DBPost).where(DBPost.collection_task_id.in_(keep_posts_ids)).values(collection_task_id=None))

            # Delete the tasks
            task_names = [ti[0] for ti in task_names_keep_info]
            stmt = (
                delete(DBCollectionTask)
                .where(DBCollectionTask.task_name.in_(task_names))
                .execution_options(synchronize_session="fetch")
            )
            session.execute(stmt)

    def add_db_collection_tasks(self, collection_tasks: list[ClientTaskConfig]) -> list[str]:
        """
        Add collection tasks to the database with conflict resolution.
        
        Handles existing task names based on task configuration:
        - overwrite: replaces existing task (optionally keeping old posts)
        - force_new_index: finds new index for grouped tasks
        - default: skips existing tasks
        """
        task_names = [t.task_name for t in collection_tasks]
        existing_names = self.check_task_names_exists(task_names)
        new_tasks_names = [t.task_name for t in collection_tasks if t.task_name not in existing_names]
        to_overwrite: list[tuple[str, bool]] = []  # (task_name, keep_posts)
        remove_tasks = []

        # Handle existing task name conflicts
        group_prefix_existing_tasks: dict[str, set[int]] = defaultdict(set)
        if existing_names:
            self.logger.debug(f"collection tasks already exist: {existing_names}")
            for t in collection_tasks:
                if t.task_name in existing_names:
                    if t.overwrite:
                        self.logger.debug(f"task '{t.task_name}' exists and set to overwrite")
                        to_overwrite.append((t.task_name, t.keep_old_posts))
                    elif t.force_new_index:
                        # Find next available index for grouped tasks
                        if not group_prefix_existing_tasks.get(t.group_prefix):
                            with self.get_session() as session:
                                stmt = select(DBCollectionTask.task_name).where(
                                    DBCollectionTask.task_name.like(f'{t.group_prefix}_%'))
                                group_prefix_existing_tasks[t.group_prefix] = set(
                                    [int(tn.removeprefix(f"{t.group_prefix}_")) for tn in
                                     session.execute(stmt).scalars().all()])
                        existing_indices = group_prefix_existing_tasks[t.group_prefix]

                        # Find next available index
                        for next_idx in range(len(existing_indices) + 1):
                            if next_idx not in existing_indices:
                                new_t_name = f"{t.group_prefix}_{next_idx}"
                                self.logger.debug(f"task will get new task_name {t.task_name} -> {new_t_name}")
                                t.task_name = new_t_name
                                existing_indices.add(next_idx)
                                new_tasks_names.append(t.task_name)
                                break
                    else:
                        self.logger.debug(f"task '{t.task_name}' exists, will be skipped")
                        remove_tasks.append(t)

            # Delete tasks marked for overwrite
            if to_overwrite:
                self.delete_tasks(to_overwrite)

        # Remove skipped tasks
        for t in remove_tasks:
            collection_tasks.remove(t)

        # Add new tasks to database
        with self.get_session() as session:
            for task in collection_tasks:
                serialized_config = (task.platform_collection_config.model_dump()
                                     if task.platform_collection_config else None)
                db_task = DBCollectionTask(
                    task_name=task.task_name,
                    platform=task.platform,
                    collection_config=task.collection_config.model_dump(exclude_defaults=True, exclude_unset=True),
                    platform_collection_config=serialized_config,
                    transient=task.transient,
                    status=task.status
                )
                session.add(db_task)
            session.commit()

            if self.logger.level <= logging.INFO:
                task_summary = new_tasks_names if len(task_names) < 50 else f"{len(task_names)} tasks"
                self.logger.info(f"Added new collection tasks: {task_summary}")
            return new_tasks_names

    def get_pending_tasks(self, include_paused_tasks: bool = False) -> list[ClientTaskConfig]:
        """Get all tasks that need to be executed."""
        states = [CollectionStatus.INIT]
        if include_paused_tasks:
            states.append(CollectionStatus.PAUSED)
        return self.get_tasks_of_states(states)

    def get_tasks_of_states(self, states: list[CollectionStatus], negate: bool = False) -> list[ClientTaskConfig]:
        """Get tasks matching the given states."""
        with self.get_session() as session:
            state_filter = DBCollectionTask.status.in_(states)
            if negate:
                state_filter = ~state_filter
            tasks = session.query(DBCollectionTask).filter(state_filter).all()
            task_objs = []
            for task in tasks:
                task_obj = ClientTaskConfig.model_validate(task)
                task_obj.test_data = task.collection_config.get('test_data')
                task_objs.append(task_obj)
            return task_objs

    def insert_posts_with_deduplication(self, posts: list[DBPost]) -> list[PostModel]:
        """
        Insert posts while guaranteeing no duplicates exist.
        """
        with self.get_session() as session:
            # Remove duplicates within the provided posts
            unique_posts = []
            posts_ids = set()
            for post in posts:
                if post.platform_id not in posts_ids:
                    unique_posts.append(post)
                    posts_ids.add(post.platform_id)

            # Filter out posts that already exist in database
            existing_ids = session.execute(
                select(DBPost.platform_id).filter(DBPost.platform_id.in_(list(posts_ids)))).scalars().all()
            filtered_posts = [post for post in unique_posts if post.platform_id not in existing_ids]

            session.add_all(filtered_posts)
            session.commit()
            return [p.model() for p in filtered_posts]

    def update_task_results(self, col_result: CollectionResult):
        """Update task with collection results."""
        with self.get_session() as session:
            task_record = session.query(DBCollectionTask).get(col_result.task.id)
            if col_result.task.transient:
                session.delete(task_record)
                return

            task_record.status = CollectionStatus.DONE
            task_record.found_items = col_result.collected_items
            task_record.added_items = len(col_result.added_posts)
            task_record.collection_duration = col_result.duration
            task_record.execution_ts = col_result.execution_ts

    def update_task_status(self, task_id: int, status: CollectionStatus):
        """Update task status in database."""
        with self.get_session() as session:
            task = session.query(DBCollectionTask).get(task_id)
            task.status = status
            session.commit()

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
