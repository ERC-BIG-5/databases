import os
from datetime import date
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Generator, Literal, Optional

from sqlalchemy import func
from sqlalchemy import select

from databases.external import CollectionStatus, SQliteConnection, DBConfig
from databases.model_conversion import PostModel
from tools.env_root import root

if TYPE_CHECKING:
    from databases.db_mgmt import DatabaseManager
from databases.db_models import DBPost, DBCollectionTask

TimeWindow = Literal["day", "month", "year"]


class TimeColumn(str, Enum):
    CREATED = "created"
    COLLECTED = "collected"


def base_data_path() -> Path:
    return root() / "data"


def filter_posts_with_existing_post_ids(posts: list[DBPost | PostModel], db_mgmt: "DatabaseManager") -> list[
    DBPost | PostModel]:
    post_ids = [p.platform_id for p in posts]
    with db_mgmt.get_session() as session:
        query = select(DBPost.platform_id).where(DBPost.platform_id.in_(post_ids))
        found_post_ids = session.execute(query).scalars().all()
        db_mgmt.logger.debug(f"filter out posts with ids: {found_post_ids}")
    return list(filter(lambda p: p.platform_id not in found_post_ids, posts))


def reset_task_states(db_mgmt: "DatabaseManager", tasks_ids: list[int]) -> None:
    with db_mgmt.get_session() as session:
        session.query(DBCollectionTask).filter(DBCollectionTask.id.in_(tasks_ids)).update(
            {DBCollectionTask.status: CollectionStatus.INIT},
            synchronize_session="fetch"
        )


def check_platforms(db_mgmt: "DatabaseManager", from_tasks: bool = True) -> set[str]:
    """
    return the set of platforms of a database
    :param db_mgmt: database-manager
    :param from_tasks: use task table (otherwise post table)
    :return: set of platforms (string)
    """
    with db_mgmt.get_session() as session:
        if from_tasks:
            model = DBCollectionTask
        else:
            model = DBPost
        return set(p[0] for p in session.query(model.platform))


def file_size(db_mgmt: "DatabaseManager") -> int:
    if isinstance(db_mgmt.config.db_connection, SQliteConnection):
        file_path = db_mgmt.config.db_connection.db_path
        return os.stat(file_path).st_size
    else:
        return 0


def get_posts(db: "DatabaseManager") -> Generator[PostModel, None, None]:
    with db.get_session() as session:
        for post in session.execute(select(DBPost)).scalars():
            yield post.model()


def get_posts_by_day(db: "DatabaseManager") -> Generator[tuple[date, int], None, None]:
    with db.get_session() as session:
        query = select(
            func.date(DBPost.date_created).label('day'),
            func.count().label('count')
        ).group_by(
            func.date(DBPost.date_created)
        )

        # Execute the query and return the results
        result = session.execute(query).all()
        for date_, count in result:
            yield date_, count


def get_posts_by_period(db: "DatabaseManager",
                        period: TimeWindow,
                        time_col: TimeColumn) -> Generator[
    tuple[str, int], None, None]:

    time_col_m = DBPost.date_created if time_col == TimeColumn.CREATED else DBPost.date_collected

    if period == "day":
        group_expr = func.strftime('%Y-%m-%d', time_col_m).label('period')

    elif period == "month":
        # Format as YYYY-MM (year-month)
        group_expr = func.strftime('%Y-%m', time_col_m).label('period')

    elif period == "year":
        # Format as YYYY (year)
        group_expr = func.strftime('%Y', time_col_m).label('period')
    else:
        raise ValueError(f"Unsupported time window: {period}")

    with db.get_session() as session:
        query = select(
            group_expr,
            func.count().label('count')
        ).group_by(group_expr).order_by(group_expr)

        # Execute the query and return the results
        result = session.execute(query).all()
        for date_, count in result:
            yield date_, count


def count_posts(db_manager: "DatabaseManager") -> int:
    """
    Get the total count of posts in the database.

    :param db_manager: DatabaseManager instance
    :return: Total number of posts in the database
    """
    with db_manager.get_session() as session:
        count = session.execute(select(func.count()).select_from(DBPost)).scalar()
        return count


if __name__ == "__main__":
    pass
    # from tools.env_root import root
    # from databases.db_mgmt import DatabaseManager
    # root("/home/rsoleyma/projects/platforms-clients")
    # db = DatabaseManager.sqlite_db_from_path(root() / "data/youtube2024.sqlite", False)
