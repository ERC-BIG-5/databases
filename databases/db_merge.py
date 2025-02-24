from timeit import timeit

import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Generator

from sqlalchemy import select
from sqlalchemy.orm import Session

from tqdm import tqdm

from databases.db_mgmt import DatabaseManager
from databases.db_models import DBPost, DBCollectionTask
from databases.db_utils import filter_posts_with_existing_post_ids
from databases.external import DBConfig, SQliteConnection, CollectionStatus
from databases.model_conversion import PostModel, CollectionTaskModel


# RAISE_DB_ERROR = True

@dataclass(frozen=True)
class TaskHash:
    task_name: str
    added_items: int
    time_added: datetime
    status: CollectionStatus


class DBMerger:
    BATCH_SIZE = 100

    def __init__(self, db_path: Path, platform: str, add_fake_collection_task: bool = False):
        self.db_path = db_path
        self.db = DatabaseManager(DBConfig(db_connection=SQliteConnection(db_path=db_path)))
        self.batch: list[PostModel] = []
        self.platforms = platform
        if add_fake_collection_task:
            self.add_fake_collection_task()
        # else:
        #     raise NotImplementedError("add_fake_collection_task is not implemented.")

    @staticmethod
    def db_for_path(db_path: Path) -> DatabaseManager:
        return DatabaseManager(DBConfig(db_connection=SQliteConnection(db_path=db_path)))

    def add_fake_collection_task(self):
        with self.db.get_session() as session:
            session.add(DBCollectionTask(task_name="fake_collection", platform=self.platforms, collection_config={}))

    def add_post(self, post: PostModel, orig_db_name: Path):
        self.batch.append(post)

        if len(self.batch) >= self.BATCH_SIZE:
            posts: list[PostModel] = filter_posts_with_existing_post_ids(self.batch, self.db)
            db_posts: list[DBPost] = []
            for post in posts:
                md = post.metadata_content
                md.orig_db_conf = (orig_db_name.as_posix(), post.collection_task_id)
                post.collection_task_id = 1
                post_d = post.model_dump(exclude={"id"})
                db_posts.append(DBPost(**post_d))

            self.db.submit_posts(db_posts)
            self.batch.clear()

    @staticmethod
    def get_tasks(db: DatabaseManager) -> Generator[PostModel, None, None]:
        with db.get_session() as session:
            query = select(DBCollectionTask)

            # Execute the query and return the results
            result = session.execute(query).scalars()
            for task in result:
                yield task.model()

    @staticmethod
    def get_posts_w_task(db: DatabaseManager) -> Generator[tuple[PostModel, CollectionTaskModel], None, None]:
        with db.get_session() as session:
            query = select(DBPost, DBCollectionTask).where(DBPost.collection_task_id == DBCollectionTask.id)

            # Execute the query and return the results
            result = session.execute(query)
            for post, task in result:
                yield post.model(), task.model()

    @staticmethod
    def get_tasks_with_posts(db: DatabaseManager) -> Generator[tuple[CollectionTaskModel, list[PostModel]], None, None]:
        with db.get_session() as session:
            # First get all tasks
            tasks_query = select(DBCollectionTask)
            tasks = session.execute(tasks_query).scalars()

            for task in tasks:
                # For each task, get its associated posts
                posts_query = select(DBPost).where(DBPost.collection_task_id == task.id)
                posts = session.execute(posts_query).scalars()

                # Convert both task and posts to their models
                yield task.model(), [post.model() for post in posts]

    @staticmethod
    def get_posts(db: DatabaseManager) -> Generator[PostModel, None, None]:
        with db.get_session() as session:
            query = select(DBPost)
            # Execute the query and return the results
            result = session.execute(query).scalars()
            for post in result:
                yield post.model()

    def merge(self, dbs: list[Path]):

        to_skip = 0
        added_tasks: set[str] = set()

        def filter_existing_posts(session : Session, posts: list[PostModel]) -> list[PostModel]:
            # Check which posts exist in a single query
            platform_ids = [post.platform_id for post in posts]
            existing_post_ids = {
                pid[0] for pid in session.execute(
                    select(DBPost.platform_id).where(DBPost.platform_id.in_(platform_ids))
                ).fetchall()
            }

            # Filter out posts that already exist
            new_posts = [
                post for post in posts
                if post.platform_id not in existing_post_ids
            ]
            return new_posts

        with self.db.get_session() as session:
            for db_path in dbs:
                print(f"Processing database: {db_path}")

                for task, posts in self.get_tasks_with_posts(self.db_for_path(db_path)):

                    new_posts = filter_existing_posts(session, posts)
                    num_new_posts = len(new_posts)

                    # Check if task already exists by task_name
                    existing_task: DBCollectionTask = session.execute(
                        select(DBCollectionTask).where(DBCollectionTask.task_name == task.task_name)
                    ).scalar()

                    if existing_task:
                        # Add new posts to existing task
                        for post in new_posts:
                            post.collection_task_id = existing_task.id
                            post_data = post.model_dump(exclude={"id"})
                            new_post = DBPost(**post_data)
                            session.add(new_post)
                        existing_task.found_items += num_new_posts
                        existing_task.added_items += num_new_posts
                    else:
                        new_task = DBCollectionTask(**task.model_dump(exclude={"id"}))
                        new_task.found_items = num_new_posts
                        new_task.added_items = num_new_posts
                        session.add(new_task)
                        session.flush()

                        added_tasks.add(task.task_name)

                        # Add all new posts
                        for post in new_posts:
                            post.collection_task_id = new_task.id
                            post_data = post.model_dump(exclude={"id"})
                            new_post = DBPost(**post_data)
                            session.add(new_post)

                    to_skip += len(posts) - len(new_posts)
                    session.commit()

        print(f"Skipped {to_skip} duplicate posts")

    @staticmethod
    def find_conflicting_tasks(dbs: list[Path]) -> dict[TaskHash, list[Path]]:
        all_tasks: dict[TaskHash, list[str]] = {}

        db_names = []
        db_name_refs: dict[Path, str] = {}
        for db_path in dbs:
            db_names.append(db_path.name)
            db_name_refs[db_path] = db_path.name

        if len(set(db_names)) != len(db_names):
            db_name_refs = {p: p.as_posix() for p in dbs}

        # print(db_name_refs)

        for db_path in dbs:
            print(f"processing: {db_path}")
            db = DBMerger.db_for_path(db_path)

            db_ref = db_name_refs[db_path]
            for res in DBMerger.get_tasks(db):
                # print(res)
                th = TaskHash(**res.model_dump(include={"task_name", "added_items", "time_added", "status"}))
                all_tasks.setdefault(th, []).append(db_ref)

        conflicting_tasks: dict[TaskHash, list[Path]] = {}
        db_name_refs_rev = {v: k for k, v in db_name_refs.items()}
        for k, v in all_tasks.items():
            if len(v) > 1:
                print(k, v)

                conflicting_tasks[k] = [db_name_refs_rev[db] for db in v]

        print(len(all_tasks))
        return conflicting_tasks

    @staticmethod
    def find_conflicting_posts(dbs: list[Path], with_tqdm: bool = True):
        all_posts: dict[str, list[str]] = {}

        db_names = []
        db_name_refs: dict[Path, str] = {}
        for db_path in dbs:
            db_names.append(db_path.name)
            db_name_refs[db_path] = db_path.name

        if len(set(db_names)) != len(db_names):
            db_name_refs = {p: p.as_posix() for p in dbs}

        # print(db_name_refs)

        for db_path in dbs:
            print(f"processing: {db_path}")
            db = DBMerger.db_for_path(db_path)

            db_ref = db_name_refs[db_path]
            c = 0

            if with_tqdm:
                iter_ = tqdm(DBMerger.get_posts(db))
            else:
                iter_ = DBMerger.get_posts(db)

            for res in iter_:
                platform_id = res.platform_id
                all_posts.setdefault(platform_id, []).append(db_ref)
                c += 1
            print(c)
        conflicting_posts: dict[str, list[str]] = {}
        db_name_refs_rev = {v: k for k, v in db_name_refs.items()}

        for k, v in all_posts.items():
            if len(v) > 1:
                # print(k, v)
                conflicting_posts[k] = [db_name_refs_rev[db].as_posix() for db in v]

        print(f"total: {len(all_posts)} conflicts: {len(conflicting_posts)}")
        return conflicting_posts


if __name__ == "__main__":
    # merger = DBMerger(Path("/home/rsoleyma/projects/platforms-clients/data/col_db/youtube/remote/merge.sqlite"),
    #                   "youtube")
    # conflicts = merger.find_conflicting_tasks(
    #     [Path("/home/rsoleyma/projects/platforms-clients/data/col_db/tiktok/tiktok_vm.sqlite"),
    #      Path("/home/rsoleyma/projects/platforms-clients/data/col_db/tiktok/tiktok_local.sqlite")])
    # print(conflicts)

    # base = Path("/home/rsoleyma/projects/platforms-clients/data/col_db/youtube/remote")
    # merger.merge([
    #     base / "youtube_2022.sqlite",
    #     base / "youtube_2022sqlite"
    # ])

    # merge done, 23.02
    # base = Path("/home/rsoleyma/projects/platforms-clients/data/col_db/tiktok/")
    # merger = DBMerger(base /"merge.sqlite", "tiktok")
    # merger.merge([
    #     base / "tiktok_local.sqlite",
    #     base / "rm/tiktok.sqlite"
    # ])
    # Standard COUNT(*) - counts all rows

    pass



    # conflicts = merger.find_conflicting_posts(
    #     [Path("/home/rsoleyma/projects/platforms-clients/data/col_db/tiktok/tiktok_vm.sqlite"),
    #      Path("/home/rsoleyma/projects/platforms-clients/data/col_db/tiktok/tiktok_local.sqlite")])
    #
    # print(len(conflicts))
    # json.dump(conflicts, open("tiktok_conflicts.json","w"))
    # conflicts = json.load(open("tiktok_conflicts.json"))
    # print(len(conflicts))
