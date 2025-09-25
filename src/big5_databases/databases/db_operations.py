import re
from collections import defaultdict
from typing import TYPE_CHECKING, Generator, Optional

from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.orm import Session

from .external import CollectionStatus
from .model_conversion import PostModel, CollectionTaskModel

if TYPE_CHECKING:
    from .db_mgmt import DatabaseManager
from .db_models import DBPost, DBCollectionTask


def filter_posts_with_existing_post_ids(posts: list[DBPost | PostModel],
                                        session: Optional[Session] = None,
                                        db: Optional["DatabaseManager"] = None) -> list[
    DBPost | PostModel]:
    """Filter out posts that already exist in the database by platform_id."""
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
    """Reset collection task states to INIT for given task IDs."""
    with db.get_session() as session:
        session.query(DBCollectionTask).filter(DBCollectionTask.id.in_(tasks_ids)).update(
            {DBCollectionTask.status: CollectionStatus.INIT},
            synchronize_session="fetch"
        )



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


def count_states(db: "DatabaseManager") -> dict[str, int]:
    """
    Count DBCollectionTask grouped by status.
    
    :param db: DatabaseManager instance
    :return: Dictionary mapping status names to counts
    """
    with db.get_session() as session:
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
    Get task-groups of a database. For each group, return a list of id,status pairs.

    :param db: DatabaseManager instance
    :return: Dictionary mapping group prefixes to lists of (id, status) tuples
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