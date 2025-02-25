import os
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import select

from databases.external import CollectionStatus, SQliteConnection
from databases.model_conversion import PostModel

if TYPE_CHECKING:
    from databases.db_mgmt import DatabaseManager
from databases.db_models import DBPost, DBCollectionTask


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