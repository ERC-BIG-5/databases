import logging
from collections import defaultdict
from pathlib import Path
from typing import Optional, Sequence, Union, Literal

from deprecated.classic import deprecated
from sqlalchemy import exists
from sqlalchemy import select
from sqlalchemy.sql.expression import delete, update
from sqlalchemy.exc import IntegrityError
from tools.project_logging import get_logger

from .db_mgmt import DatabaseManager
from .db_models import DBCollectionTask, DBPost, CollectionResult
from .db_settings import SqliteSettings
from .external import CollectionStatus, DatabaseBasestats
from .external import DBConfig, PlatformDBConfig, SQliteConnection, ClientTaskConfig
from .model_conversion import PostModel
from . import db_analytics, db_operations


class PlatformDB(DatabaseManager):
    """
    Platform-specific database manager that inherits from DatabaseManager.

    This class extends DatabaseManager to provide platform-specific functionality
    for managing social media platform databases, including task management,
    post handling, and database operations specific to data collection workflows.

    Parameters
    ----------
    config : PlatformDBConfig
        Platform-specific database configuration containing platform name,
        connection details, and table type specifications.

    Attributes
    ----------
    platform : str
        Name of the social media platform this database manages.
    logger : logging.Logger
        Logger instance for platform database operations.
    """

    @classmethod
    def create_default_config(cls, platform: str, table_type: Literal["posts", "process"] = "posts") -> PlatformDBConfig:
        """
        Create a default configuration for a platform.

        Parameters
        ----------
        platform : str
            Name of the social media platform.
        table_type : Literal["posts", "process"], optional
            Type of tables to create, by default "posts".

        Returns
        -------
        PlatformDBConfig
            Default configuration object for the specified platform.
        """
        return PlatformDBConfig(
            platform=platform,
            db_connection=SQliteConnection(
                db_path=(SqliteSettings().default_sqlite_dbs_base_path / f"{platform}.sqlite").as_posix()
            ),
            table_type=table_type
        )

    @staticmethod
    def sqlite_db_from_path(platform: str,
                            path: str | Path,
                            create: bool = False,
                            table_type: Literal["posts", "process"] = "posts") -> "PlatformDB":
        """
        Create a PlatformDB instance from a SQLite database path.

        Parameters
        ----------
        platform : str
            Name of the social media platform.
        path : str or Path
            Path to the SQLite database file.
        create : bool, optional
            Whether to create the database if it doesn't exist, by default False.
        table_type : Literal["posts", "process"], optional
            Type of tables to create, by default "posts".

        Returns
        -------
        PlatformDB
            New PlatformDB instance configured for the SQLite database.
        """
        config = PlatformDBConfig(
            platform=platform,
            db_connection=SQliteConnection(db_path=path),
            create=create,
            require_existing_parent_dir=True,
            table_type=table_type
        )
        return PlatformDB(config)

    def __init__(self, config: PlatformDBConfig):
        """
        Initialize the PlatformDB with platform-specific configuration.

        Parameters
        ----------
        config : PlatformDBConfig
            Platform-specific database configuration containing platform name,
            connection details, table specifications, and other settings.
        """
        self.platform = config.platform

        # Set platform-specific tables based on table_type
        config.tables = config.tables

        super().__init__(config)
        self.logger = get_logger(__file__)

    @property
    @deprecated(reason="was added in the platform_manager constructor. but not needed I suppose")
    def manager(self):
        """
        Get the manager property (deprecated).

        Returns
        -------
        None
            Always returns None as this property is deprecated.

        Notes
        -----
        This property is deprecated and scheduled for removal.
        """
        return None

    def check_task_name_exists(self, task_name: str) -> bool:
        """
        Check if a task name already exists in the database.

        Parameters
        ----------
        task_name : str
            Name of the task to check.

        Returns
        -------
        bool
            True if the task name exists, False otherwise.
        """
        with self.get_session() as session:
            return session.query(exists().where(DBCollectionTask.task_name == task_name)).scalar()

    def check_task_names_exists(self, task_names: list[str]) -> list[str]:
        """
        Check which task names from a list already exist in the database.

        Parameters
        ----------
        task_names : list[str]
            List of task names to check.

        Returns
        -------
        list[str]
            List of task names that already exist in the database.
        """
        with self.get_session() as session:
            existing_tasks = session.scalars(
                select(DBCollectionTask.task_name).where(DBCollectionTask.task_name.in_(task_names))).all()
            return existing_tasks

    def delete_tasks(self, task_names_keep_info: list[tuple[str, bool]]) -> None:
        """
        Delete tasks from the database with option to keep associated posts.

        Parameters
        ----------
        task_names_keep_info : list[tuple[str, bool]]
            List of tuples containing (task_name, keep_posts) where:
            - task_name: Name of the task to delete
            - keep_posts: Whether to keep posts associated with the task

        Notes
        -----
        If keep_posts is True, posts associated with the task will have their
        collection_task_id set to None instead of being deleted.
        """

        with self.get_session() as session:
            keep_posts_of_tasks = [ti[0] for ti in task_names_keep_info if ti[1]]

            keep_posts_ids = session.query(DBCollectionTask.id).where(
                DBCollectionTask.task_name.in_(keep_posts_of_tasks)).all()
            keep_posts_ids = [k[0] for k in keep_posts_ids]
            session.execute(
                update(DBPost).where(DBPost.collection_task_id.in_(keep_posts_ids)).values(collection_task_id=None))

            task_names = [ti[0] for ti in task_names_keep_info]
            stmt = (
                delete(DBCollectionTask)
                .where(DBCollectionTask.task_name.in_(task_names))
                .execution_options(synchronize_session="fetch")
            )
            session.execute(stmt)

    def add_db_collection_tasks(self, collection_tasks: list["ClientTaskConfig"]) -> list[str]:
        """
        Add collection tasks to the database with conflict resolution.

        Parameters
        ----------
        collection_tasks : list[ClientTaskConfig]
            List of collection task configurations to add.

        Returns
        -------
        list[str]
            List of task names that were successfully added to the database.

        Notes
        -----
        For existing tasks, behavior depends on task configuration:
        - If overwrite=True: Existing task is deleted and replaced
        - If force_new_index=True: Task gets a new indexed name within its group
        - Otherwise: Existing task is skipped
        """
        task_names = [t.task_name for t in collection_tasks]
        existing_names = self.check_task_names_exists(task_names)
        new_tasks = list(filter(lambda t: t.task_name not in existing_names, collection_tasks))
        new_tasks_names = [t.task_name for t in new_tasks]
        to_overwrite: list[tuple[str, bool]] = []  # will be deleted first tuple[task-name, keep-posts]...

        remove_tasks = []
        # check if existing names can be overwritten. then. delete
        group_prefix_existing_tasks: dict[str, set[int]] = defaultdict(set)
        if existing_names:
            self.logger.debug(f"client collection tasks exists already: {existing_names}")
            for t in collection_tasks:
                if t.task_name in existing_names:
                    if t.overwrite:
                        self.logger.debug(f"task '{t.task_name}' exists and set to overwrite")
                        to_overwrite.append((t.task_name, t.keep_old_posts))
                    elif t.force_new_index:
                        if not group_prefix_existing_tasks.get(t.group_prefix):
                            with self.get_session() as session:
                                stmt = select(DBCollectionTask.task_name).where(
                                    DBCollectionTask.task_name.like(f'{t.group_prefix}_%'))
                                group_prefix_existing_tasks[t.group_prefix] = set(
                                    [int(tn.removeprefix(f"{t.group_prefix}_")) for tn in
                                     session.execute(stmt).scalars().all()])
                        existing_indices = group_prefix_existing_tasks[t.group_prefix]

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
            if to_overwrite:
                self.delete_tasks(to_overwrite)

        for t in remove_tasks:
            collection_tasks.remove(t)

        with self.get_session() as session:
            # specific function. refactor out
            for task in collection_tasks:
                if task.platform_collection_config:
                    serialized_config = (task.platform_collection_config.model_dump()
                                        if hasattr(task.platform_collection_config, 'model_dump')
                                        else task.platform_collection_config)
                else:
                    serialized_config = None
                task = DBCollectionTask(
                    task_name=task.task_name,
                    platform=task.platform,
                    collection_config=task.collection_config.model_dump(exclude_defaults=True, exclude_unset=True),
                    platform_collection_config=serialized_config,
                    transient=task.transient,
                    status=task.status
                )
                session.add(task)
            session.commit()
            if self.logger.level <= logging.INFO:
                task_s = new_tasks_names if (tn_le := len(task_names)) < 50 else tn_le
                self.logger.info(f"Added new client collection tasks: {task_s}")
            return new_tasks_names

    def get_pending_tasks(self, include_paused_tasks: bool = False) -> list[ClientTaskConfig]:
        """
        Get all tasks that need to be executed.

        Parameters
        ----------
        include_paused_tasks : bool, optional
            Whether to include paused tasks in the result, by default False.

        Returns
        -------
        list[ClientTaskConfig]
            List of tasks that are pending execution.
        """
        return self.get_tasks_of_states([
                                            CollectionStatus.INIT
                                        ] + ([CollectionStatus.PAUSED] if include_paused_tasks else []))

    def get_tasks_of_states(self,
                            states: list[CollectionStatus],
                            negate: bool = False) -> list[ClientTaskConfig]:
        """
        Get tasks filtered by their status states.

        Parameters
        ----------
        states : list[CollectionStatus]
            List of collection statuses to filter by.
        negate : bool, optional
            If True, return tasks NOT in the specified states, by default False.

        Returns
        -------
        list[ClientTaskConfig]
            List of tasks matching the status criteria.
        """
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

    @deprecated(reason="replaced using db_mgmt.safe_submit_posts")
    def insert_posts(self, posts: list[DBPost]) -> list[PostModel]:
        """
        Insert posts ensuring no duplicates exist (deprecated).

        Parameters
        ----------
        posts : list[DBPost]
            List of database post objects to insert.

        Returns
        -------
        list[PostModel]
            List of successfully inserted posts as PostModel objects.

        Notes
        -----
        This method is deprecated. Use safe_submit_posts instead.
        """
        # Store posts
        with self.get_session() as session:
            # try:
            unique_posts = []
            posts_ids = set()
            for post in posts:
                if post.platform_id not in posts_ids:
                    unique_posts.append(post)
                    posts_ids.add(post.platform_id)

            # todo, there must be helper for this?!
            existing_ids = session.execute(
                select(DBPost.platform_id).filter(DBPost.platform_id.in_(list(posts_ids)))).scalars().all()
            posts = list(filter(lambda post_: post_.platform_id not in existing_ids, unique_posts))

            session.add_all(posts)
            session.commit()
            # todo ADD USERS
            return [p.model() for p in posts]

    def update_task_results(self, col_result: CollectionResult):
        """
        Update task with collection results.

        Parameters
        ----------
        col_result : CollectionResult
            Collection result object containing task execution details.

        Notes
        -----
        If the task is transient, it will be deleted from the database.
        Otherwise, task status and metrics are updated.
        """
        # update task status
        with self.get_session() as session:
            task_record = session.query(DBCollectionTask).get(col_result.task.id)
            if col_result.task.transient:
                session.delete(task_record)
            else:
                task_record.status = CollectionStatus.DONE
                task_record.found_items = col_result.collected_items
                task_record.added_items = len(col_result.added_posts)
                task_record.collection_duration = col_result.duration
                task_record.execution_ts = col_result.execution_ts
            session.commit()

    def update_task_status(self, task_id: int, status: CollectionStatus):
        """
        Update task status in database.

        Parameters
        ----------
        task_id : int
            ID of the task to update.
        status : CollectionStatus
            New status for the task.
        """
        with self.get_session() as session:
            task = session.query(DBCollectionTask).get(task_id)
            task.status = status
            session.commit()

    def reset_running_tasks(self,
                            states: Sequence[CollectionStatus] = (CollectionStatus.RUNNING,
                                                               CollectionStatus.ABORTED)) -> int:
        """
        Reset tasks in specified states back to INIT status.

        Parameters
        ----------
        states : Sequence[CollectionStatus], optional
            Task states to reset, by default (RUNNING, ABORTED).

        Returns
        -------
        int
            Number of tasks that were reset.
        """

        with self.get_session() as session:
            tasks = session.query(DBCollectionTask).filter(
                DBCollectionTask.status.in_(list(states))
            ).all()

            c = 0
            for t in tasks:
                t.status = CollectionStatus.INIT
                c += 1
            session.commit()
        self.logger.debug(f"{self.platform}: Set tasks to pause: {c} tasks")
        return c

    def safe_submit_posts(self, posts: list[Union[DBPost, PostModel]]) -> list[PostModel]:
        """
        Safely submit posts, handling both DBPost and PostModel objects.

        Parameters
        ----------
        posts : list[Union[DBPost, PostModel]]
            List of posts to submit, can be DBPost or PostModel objects.

        Returns
        -------
        list[PostModel]
            List of successfully submitted posts as PostModel objects.

        Notes
        -----
        This method handles integrity errors by filtering out existing posts
        and retrying submission. PostModel objects are converted to DBPost
        objects before submission.
        """
        # Convert PostModel objects to DBPost if needed
        db_posts = []
        for post in posts:
            if isinstance(post, PostModel):
                # Convert PostModel to DBPost
                db_post = DBPost(
                    platform_id=post.platform_id,
                    platform=post.platform,
                    content=post.content,
                    post_url=post.post_url,
                    date_created=post.date_created,
                    post_type=post.post_type,
                    metadata_content=post.metadata_content.model_dump() if hasattr(post.metadata_content, 'model_dump') else post.metadata_content or {},
                    collection_task_id=post.collection_task_id,
                )
                db_posts.append(db_post)
            else:
                db_posts.append(post)

        submit_posts = db_posts
        while True:
            try:
                submitted_posts = self._submit_posts(submit_posts)
                return submitted_posts
            except IntegrityError:
                with self.get_session() as session:
                    filtered_posts = db_operations.filter_posts_with_existing_post_ids(submit_posts, session)
                    return [p.model() for p in filtered_posts]
            except Exception as e:
                self.logger.error(f"Error submitting posts: {str(e)}")
                return []

    def _submit_posts(self, posts: list[DBPost]) -> list[PostModel]:
        """
        Internal method to submit posts to database.

        Parameters
        ----------
        posts : list[DBPost]
            List of database post objects to submit.

        Returns
        -------
        list[PostModel]
            List of submitted posts as PostModel objects.
        """
        with self.get_session() as session:
            session.add_all(posts)
            session.commit()
            return [p.model() for p in posts]

    def insert_posts_with_deduplication(self, posts: list[DBPost]) -> list[PostModel]:
        """
        Insert posts while guaranteeing no duplicates exist.

        Parameters
        ----------
        posts : list[DBPost]
            List of database post objects to insert.

        Returns
        -------
        list[PostModel]
            List of successfully inserted posts as PostModel objects.

        Notes
        -----
        This method removes duplicates both within the input list and
        against existing database records before insertion.
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

    def update_task(self, task_id: int, status: str, found_items: int, added_items: int, duration: int):
        """
        Update task with execution results.

        Parameters
        ----------
        task_id : int
            ID of the task to update.
        status : str
            New status for the task.
        found_items : int
            Number of items found during collection.
        added_items : int
            Number of items successfully added to database.
        duration : int
            Collection duration in seconds.
        """
        with self.get_session() as session:
            task = session.query(DBCollectionTask).get(task_id)
            task.status = status
            task.found_items = found_items
            task.added_items = added_items
            task.collection_duration = int(duration * 1000)
            session.commit()

    def calc_db_content(self) -> DatabaseBasestats:
        """
        Calculate basic database statistics.

        Returns
        -------
        DatabaseBasestats
            Object containing basic database statistics including task states,
            post count, file size, and last modified timestamp.
        """
        return DatabaseBasestats(
            tasks_states=db_operations.count_states(self),
            post_count=db_analytics.count_posts(db=self),
            file_size=self._file_size(),
            last_modified=self._file_modified())

    def calc_db_stats(self) -> "MetaDatabaseStatsModel":
        """
        Calculate comprehensive database statistics.

        Returns
        -------
        MetaDatabaseStatsModel
            Object containing comprehensive database statistics including
            task states, post count, file size, last modified timestamp,
            and detailed statistical analysis.
        """
        from .external import MetaDatabaseStatsModel
        from .db_stats import generate_db_stats

        return MetaDatabaseStatsModel(
            tasks_states=db_operations.count_states(self),
            post_count=db_analytics.count_posts(db=self),
            file_size=self._file_size(),
            last_modified=self._file_modified(),
            stats=generate_db_stats(self))

    @deprecated(reason="PlatformDB now inherits from DatabaseManager. Use methods directly on PlatformDB instance.")
    def get_db_manager(self) -> DatabaseManager:
        """
        Get the underlying database manager (deprecated).

        Returns
        -------
        DatabaseManager
            Returns self since PlatformDB inherits from DatabaseManager.

        Notes
        -----
        This method is deprecated. PlatformDB now inherits from DatabaseManager,
        so all DatabaseManager methods are available directly on the PlatformDB instance.
        """
        self.logger.warning("get_db_manager() is deprecated. PlatformDB now inherits from DatabaseManager.")
        return self

    @staticmethod
    def platform_tables() -> list[str]:
        """
        Get list of platform-specific table names.

        Returns
        -------
        list[str]
            List of platform table names.

        Notes
        -----
        This method delegates to DatabaseManager.platform_tables().
        """
        return DatabaseManager.platform_tables()
