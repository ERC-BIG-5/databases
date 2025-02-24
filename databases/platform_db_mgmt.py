import sqlalchemy
from sqlalchemy import exists
from sqlalchemy import select

from databases.external import DBConfig, SQliteConnection, ClientTaskConfig
from databases.external import BASE_DATA_PATH, CollectionStatus
from databases.db_mgmt import DatabaseManager
from databases.db_models import DBCollectionTask, DBPost, CollectionResult
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

    def add_db_collection_task(self, collection_task: "ClientTaskConfig") -> bool:
        task_name = collection_task.task_name
        exists_and_overwrite = False
        if self.check_task_name_exists(task_name):
            if collection_task.test and collection_task.overwrite:
                exists_and_overwrite = True
            else:
                self.logger.info(f"client collection task exists already: {task_name}")
                return False
        with self.db_mgmt.get_session() as session:
            # specific function. refactor out
            task = DBCollectionTask(
                task_name=task_name,
                platform=collection_task.platform,
                collection_config=collection_task.model_dump()["collection_config"],
                transient=collection_task.transient,
            )
            if exists_and_overwrite:
                self.logger.debug(f"Collection task set to test and overwrite. overwriting existing task")
                prev = session.query(DBCollectionTask).where(DBCollectionTask.task_name == task_name)
                task.id = task.id
                try:
                    session.query(DBPost).where(DBPost.collection_task_id == prev.first().id).delete(
                        synchronize_session=False
                    )
                    prev.delete(synchronize_session=False)
                except sqlalchemy.exc.IntegrityError as e:
                    session.rollback()  # Rollback changes on error
                    self.logger.warning(f"Failed to delete exising task: {task.task_name} ({repr(e)}")
                    # Handle or re-raise the exception as needed
                    return False

            session.add(task)
            session.commit()
            self.logger.info(f"Added new client collection task: {task_name}")
            return True

    def get_db_manager(self) -> DatabaseManager:
        """Get the underlying database manager"""
        return self.db_mgmt

    def get_pending_tasks(self) -> list[ClientTaskConfig]:
        """Get all tasks that need to be executed"""
        return self.get_tasks_of_states([
            CollectionStatus.INIT,
            # todo, bring back PAUSED based on config
            # CollectionStatus.PAUSED
        ])

    def get_tasks_of_states(self, states: list[CollectionStatus]) -> list[ClientTaskConfig]:
        with self.db_mgmt.get_session() as session:
            tasks = session.query(DBCollectionTask).filter(
                DBCollectionTask.status.in_(states)
            ).all()
            task_objs = []
            for task in tasks:
                task_obj = ClientTaskConfig.model_validate(task)
                task_obj.test_data = task.collection_config.get('test_data')
                task_objs.append(task_obj)
            return task_objs

    def count_states(self):
        from sqlalchemy import func, case

        with self.db_mgmt.get_session() as session:
            query = (
                session.query(
                    DBCollectionTask.status,
                    func.count(DBCollectionTask.status).label('count')
                )
                .group_by(DBCollectionTask.status)
            )

            results = query.all()
            return {enum_type.name.lower(): count for enum_type, count in results}

    # todo, check when this is called... refactor, merge usage with util, and safe_insert...
    def insert_posts(self, collection: CollectionResult):
        # Store posts
        with self.db_mgmt.get_session() as session:
            # try:
            # todo filter duplicates....
            posts = collection.posts
            unique_posts = []
            posts_ids = set()
            for post in posts:
                if post.platform_id not in posts_ids:
                    unique_posts.append(post)
                    posts_ids.add(post.platform_id)

            # all_post_ids = [post.platform_id for post in posts]
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
                return posts
            task_record.status = CollectionStatus.DONE
            task_record.found_items = collection.collected_items
            task_record.added_items = len(posts)
            task_record.collection_duration = collection.duration

        self.logger.info(f"Added {len(posts)} posts to database")

    def update_task_status(self, task_id: int, status: CollectionStatus):
        """Update task status in database"""
        with self.db_mgmt.get_session() as session:
            task = session.query(DBCollectionTask).get(task_id)
            task.status = status
            session.commit()

    def pause_running_tasks(self):
        with self.db_mgmt.get_session() as session:
            tasks = session.execute(select(DBCollectionTask).filter(
                DBCollectionTask.status == CollectionStatus.RUNNING,
            )).scalars()

            c = 0
            for t in tasks:
                t.status = CollectionStatus.PAUSED
                c += 1
            self.logger.debug(f"Set tasks to pause: {c} tasks")
