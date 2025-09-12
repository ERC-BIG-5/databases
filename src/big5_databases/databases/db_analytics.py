from datetime import date
from typing import TYPE_CHECKING, Optional, TypedDict

from sqlalchemy import func
from sqlalchemy import select

from .external import TimeWindow

if TYPE_CHECKING:
    from .db_mgmt import DatabaseManager
from .db_models import DBPost, DBCollectionTask

col_per_day = TypedDict("col_per_day", {
    "tasks": int,
    "found": int,
    "added": int})


def get_posts_by_period(db: "DatabaseManager",
                        period: TimeWindow = TimeWindow.DAY) -> list[tuple[str, int]]:
    """
    Get created posts grouped by time period.

    :param db: DatabaseManager instance
    :param period: Time window for grouping (DAY, MONTH, YEAR)
    :returns: List of tuples containing (period, count) for created posts
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
                                  period: TimeWindow = TimeWindow.DAY,
                                  select_time: Optional[date] = None) -> dict[str, col_per_day]:
    """
    Get collection totals grouped by time period.

    :param db: DatabaseManager instance
    :param period: Time window for grouping (DAY, MONTH, YEAR)  
    :param select_time: Optional filter for tasks after this date
    :returns: Dictionary mapping periods to collection statistics
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
        if select_time:
            query = query.where(DBCollectionTask.execution_ts >= select_time)
        result = session.execute(query).all()

        return {str(period): col_per_day(tasks=num_tasks, found=found_total, added=added_total)
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