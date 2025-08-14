import os
import re
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Generator, Optional, TypedDict, Union

from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.orm import Session
from tools.env_root import root

from .external import CollectionStatus, SQliteConnection, TimeWindow
from .model_conversion import PostModel, CollectionTaskModel, PlatformDatabaseModel

if TYPE_CHECKING:
    from .db_mgmt import DatabaseManager
from .db_models import DBPost, DBCollectionTask

col_per_day = TypedDict("col_per_day", {
    "tasks": int,
    "found": int,
    "added": int})


def filter_posts_with_existing_post_ids(posts: list[DBPost | PostModel],
                                        session: Optional[Session] = None,
                                        db: Optional["DatabaseManager"] = None) -> list[
    DBPost | PostModel]:
    post_ids = [p.platform_id for p in posts]

    def _filter_with_session(session_: Session) -> list[DBPost | PostModel]:
        query = select(DBPost.platform_id).where(DBPost.platform_id.in_(post_ids))
        found_post_ids = session_.execute(query).scalars().all()
        # db.logger.debug(f"filter out posts with ids: {found_post_ids}")

        return [p for p in posts if p.platform_id not in found_post_ids]

    if session is not None:
        return _filter_with_session(session)

    # If only a db is provided, create a new session with context management
    with db.get_session() as new_session:
        return _filter_with_session(new_session)


def reset_task_states(db: "DatabaseManager", tasks_ids: list[int]) -> None:
    with db.get_session() as session:
        session.query(DBCollectionTask).filter(DBCollectionTask.id.in_(tasks_ids)).update(
            {DBCollectionTask.status: CollectionStatus.INIT},
            synchronize_session="fetch"
        )


def check_platforms(db: "DatabaseManager", from_tasks: bool = True) -> set[str]:
    """
    return the set of platforms of a database
    :param db: database-manager
    :param from_tasks: use task table (otherwise post table)
    :return: set of platforms (string)
    """
    with db.get_session() as session:
        if from_tasks:
            model = DBCollectionTask
        else:
            model = DBPost
        return set(p[0] for p in session.query(model.platform))


def file_size(db: Union["DatabaseManager", PlatformDatabaseModel]) -> int:
    if isinstance(db, PlatformDatabaseModel):
        file_path = db.full_path
    elif isinstance(db.config.db_connection, SQliteConnection):
        file_path = db.config.db_connection.db_path
    else:
        return 0
    return os.stat(file_path).st_size

def file_modified(db: Union["DatabaseManager", PlatformDatabaseModel]) -> float:
    if isinstance(db, PlatformDatabaseModel):
        file_path = db.full_path
    elif isinstance(db.config.db_connection, SQliteConnection):
        file_path = db.config.db_connection.db_path
    else:
        return 0
    return file_path.stat().st_mtime

def currently_open(db: Union["DatabaseManager", PlatformDatabaseModel]) -> bool:
    if isinstance(db, PlatformDatabaseModel):
        file_path = db.full_path
    elif isinstance(db.config.db_connection, SQliteConnection):
        file_path = db.config.db_connection.db_path
    else:
        return False
    wal_path = str(file_path) + '-wal'
    return os.path.exists(wal_path) and os.path.getsize(wal_path) > 0

def get_tasks_with_posts(db: "DatabaseManager") -> Generator[
    tuple[CollectionTaskModel, list[PostModel]], None, None]:
    """Get all collection tasks with their associated posts from a database."""
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


def get_posts_by_period(db: "DatabaseManager",
                        period: TimeWindow = TimeWindow.DAY) -> list[tuple[str, int]]:
    """
    Get created posts grouped by time period.

    @returns: List of tuples containing (period, count) for created posts
    """

    time_str = period.time_str

    created_expr = func.strftime(time_str, DBPost.date_created).label('created_period')

    with db.get_session() as session:
        query = (
            select(
                created_expr,
                func.count().label('count')
            )
            .group_by(created_expr)
            .order_by(created_expr)
        )

        result = session.execute(query).all()

        return [(period, count) for period, count in result]


def get_collected_posts_by_period(db: "DatabaseManager",
                                  period: TimeWindow = TimeWindow.DAY) -> dict[str,col_per_day ]:
    """
    Get collection totals grouped by time period.

    @returns: List of tuples containing (period, found_items_total, added_items_total)
    """

    time_str = period.time_str

    period_expr = func.strftime(time_str, DBCollectionTask.execution_ts).label('period')

    with db.get_session() as session:
        query = (
            select(
                period_expr,
                func.count(DBCollectionTask.id).label('task_count'),
                func.sum(DBCollectionTask.found_items).label('found_total'),
                func.sum(DBCollectionTask.added_items).label('added_total')
            )
            .where(DBCollectionTask.execution_ts.is_not(None))
            .group_by(period_expr)
            .order_by(period_expr)
        )

        result = session.execute(query).all()

        return {str(period): col_per_day(tasks=num_tasks, found=found_total,added=added_total)
                for period, num_tasks, found_total, added_total in result}


def count_posts(db: "DatabaseManager") -> int:
    """
    Get the total count of posts in the database.

    :param db: DatabaseManager instance
    :return: Total number of posts in the database
    """
    with db.get_session() as session:
        count = session.execute(select(func.count()).select_from(DBPost)).scalar()
        return count


def split_by_year(db: "DatabaseManager",
                  dest_folder: Path,
                  delete_after_success: bool = True) -> list[Path]:
    # todo
    # check if dest/platform_SPLIT_FROM_<SRC_NAME>.sqlite exists
    raise NotImplementedError()


def find_invalid_tasks(db: "DatabaseManager") -> list[int]:
    # todo.
    # tasks which are done but have relevant values None
    # tasks with number but invalid STATE (!= DONE)
    raise NotImplementedError()


def count_states(self) -> dict[str, int]:
    """
    Count DBCollectionTask grouped by status
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


def find_tasks_groups(db: "DatabaseManager") -> dict[str, list[tuple[int, CollectionStatus]]]:
    """
    get task-groups of a database. for each group, return a list of id,status pairs

    :param db:
    :return:
    """
    group_index_pattern = r'(\d+)$'
    groups = defaultdict(list)

    with db.get_session() as session:
        for task_data in session.execute(select(DBCollectionTask.task_name, DBCollectionTask.status)):
            name, status = task_data
            index_match = re.search(group_index_pattern, name)
            if index_match:
                group_id = index_match.group(1)
                # Get the prefix by removing the index from the end
                prefix = name[:name.rfind(group_id)]
                # Convert group_id to integer and add to list for this prefix
                groups[prefix].append((int(group_id), status))

    for prefix in groups:
        groups[prefix].sort()

    return dict(groups)


def reorder_posts(db: "DatabaseManager") -> None:
    # todo implement
    raise NotImplementedError()


if __name__ == "__main__":
    from big5_databases.databases.db_mgmt import DatabaseManager

    root("/home/rsoleyma/projects/platforms-clients")
    pass
    db = DatabaseManager.sqlite_db_from_path(root() / "data/col_db/youtube/from_twitter_db.sqlite", False)
    print(find_tasks_groups(db))
