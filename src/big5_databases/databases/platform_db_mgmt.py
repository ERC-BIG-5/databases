from collections import defaultdict

import sqlalchemy
from sqlalchemy import exists
from sqlalchemy import select
from sqlalchemy.sql.expression import delete, update

from .external import DBConfig, SQliteConnection, ClientTaskConfig
from .external import BASE_DATA_PATH, CollectionStatus
from .db_mgmt import DatabaseManager
from .db_models import DBCollectionTask, DBPost, CollectionResult, Base
from tools.project_logging import get_logger


class PlatformDB:
    """
    Singleton class to manage platform-specific database connections
    """

    @classmethod
    def get_platform_default_db(cls, platform: str) -> DBConfig:
        return DBConfig(db_connection=SQliteConnection(
            db_path=(BASE_DATA_PATH / f"{platform}.sqlite").as_posix()
        ))

    def __init__(self, platform: str, db_config: DBConfig = None):
        # Only initialize if this is a new instance
        self.platform = platform
        self.db_config = db_config or self.get_platform_default_db(platform)
        self.db_mgmt = DatabaseManager(self.db_config)
        self.logger = get_logger(__file__)
        self.initialized = True

    def check_task_name_exists(self, task_name: str) -> bool:
        with self.db_mgmt.get_session() as session:
            return session.query(exists().where(DBCollectionTask.task_name == task_name)).scalar()

    def check_task_names_exists(self, task_names: list[str]) -> list[str]:
        with self.db_mgmt.get_session() as session:
            existing_tasks = session.scalars(
                select(DBCollectionTask.task_name).where(DBCollectionTask.task_name.in_(task_names))).all()
            return existing_tasks

    def delete_tasks(self, task_names_keep_info: list[tuple[str, bool]]) -> None:
        """
        get a list of tuples: str,bool: taskname, keep-posts
        """

        with self.db_mgmt.get_session() as session:
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
        If the taskname is not in the database, a task goes straight in
        For those existing there are multiple options, based on how the task is configured:
        - overwrite the existing task (with suboption of keeping the old posts nevertheless)
        - force_new_index, checks for an alternative index, in case the task comes from a group. (this has to be used wisely)
        """
        task_names = [t.task_name for t in collection_tasks]
        existing_names = self.check_task_names_exists(task_names)
        new_tasks = list(filter(lambda t: t.task_name not in existing_names, collection_tasks))
        new_tasks_names = [t.task_name for t in new_tasks]
        to_overwrite: list[tuple[str, bool]] = []  # will be deleted first tuple[task-name, keep-posts]...

        remove_tasks = []
        # check if existing names can be overwritten. then. delete
        group_prefix_existing_tasks:dict[str,set[int]] = defaultdict(set)
        if existing_names:
            self.logger.debug(f"client collection tasks exists already: {existing_names}")
            for t in collection_tasks:
                if t.task_name in existing_names:
                    if t.overwrite:
                        self.logger.debug(f"task '{t.task_name}' exists and set to overwrite")
                        to_overwrite.append((t.task_name, t.keep_old_posts))
                    elif t.force_new_index:
                        if not group_prefix_existing_tasks.get(t.group_prefix):
                            with self.db_mgmt.get_session() as session:
                                stmt = select(DBCollectionTask.task_name).where(DBCollectionTask.task_name.like(f'{t.group_prefix}_%'))
                                group_prefix_existing_tasks[t.group_prefix] = set([int(tn.removeprefix(f"{t.group_prefix}_")) for tn in session.execute(stmt).scalars().all()])
                        existing_indices = group_prefix_existing_tasks[t.group_prefix]

                        for next_idx in range(len(existing_indices)+1):
                            if next_idx not in existing_indices:
                                new_t_name = f"{t.group_prefix}_{next_idx}"
                                self.logger.debug(f"task will get new task_name {t.task_name} -> {new_t_name}")
                                t.task_name = new_t_name
                                existing_indices.add(next_idx)
                        pass
                    else:
                        self.logger.debug(f"task '{t.task_name}' exists, will be skipped")
                        remove_tasks.append(t)
            if to_overwrite:
                self.delete_tasks(to_overwrite)

        for t in remove_tasks:
            collection_tasks.remove(t)

        with self.db_mgmt.get_session() as session:
            # specific function. refactor out
            for task in collection_tasks:
                if task.platform_collection_config:
                    serialized_config = task.platform_collection_config.model_dump()
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
            self.logger.info(f"Added new client collection tasks: {new_tasks_names}")

            return new_tasks_names

    def get_db_manager(self) -> DatabaseManager:
        """Get the underlying database manager"""
        return self.db_mgmt

    def get_pending_tasks(self, include_paused_tasks: bool = False) -> list[ClientTaskConfig]:
        """Get all tasks that need to be executed"""
        return self.get_tasks_of_states([
                                            CollectionStatus.INIT
                                        ] + ([CollectionStatus.PAUSED] if include_paused_tasks else []))

    def get_tasks_of_states(self,
                            states: list[CollectionStatus],
                            negate: bool = False) -> list[ClientTaskConfig]:
        with self.db_mgmt.get_session() as session:
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

    # todo, check when this is called... refactor, merge usage with util, and safe_insert...
    def insert_posts(self, collection: CollectionResult):
        # Store posts
        with self.db_mgmt.get_session() as session:
            # try:
            posts = collection.posts
            unique_posts = []
            posts_ids = set()
            for post in posts:
                if post.platform_id not in posts_ids:
                    unique_posts.append(post)
                    posts_ids.add(post.platform_id)

            # todo, there must be helper for this?!
            existing_ids = session.execute(
                select(DBPost.platform_id).filter(DBPost.platform_id.in_(list(posts_ids)))).scalars().all()
            posts = list(filter(lambda post: post.platform_id not in existing_ids, unique_posts))

            session.add_all(posts)
            session.commit()
            collection.added_posts = [p.model() for p in posts]
            # todo ADD USERS

        # update task status
        with self.db_mgmt.get_session() as session:
            task_record = session.query(DBCollectionTask).get(collection.task.id)
            if task_record.transient:
                for post in posts:
                    post.collection_task_id = None
                session.delete(task_record)
            task_record.status = CollectionStatus.DONE
            task_record.found_items = collection.collected_items
            task_record.added_items = len(posts)
            task_record.collection_duration = collection.duration
            task_record.execution_ts = collection.execution_ts

        self.logger.info(f"Added {len(posts)} posts to database (from [{len(collection.posts)}])")

    def update_task_status(self, task_id: int, status: CollectionStatus):
        """Update task status in database"""
        with self.db_mgmt.get_session() as session:
            task = session.query(DBCollectionTask).get(task_id)
            task.status = status
            session.commit()

    def reset_running_tasks(self):
        with self.db_mgmt.get_session() as session:
            tasks = session.execute(select(DBCollectionTask).filter(
                DBCollectionTask.status == CollectionStatus.RUNNING,
            )).scalars()

            c = 0
            for t in tasks:
                t.status = CollectionStatus.INIT
                c += 1
            self.logger.debug(f"{self.platform}: Set tasks to pause: {c} tasks")

    @staticmethod
    def platform_tables() -> list[str]:
        return DatabaseManager.platform_tables()
