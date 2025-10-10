from pathlib import Path
from typing import TYPE_CHECKING, Generator

from sqlalchemy import select, delete

from big5_databases.databases.c_db_merge import MergeStats, process_collection_task
from big5_databases.databases.db_mgmt import DatabaseManager
from big5_databases.databases.db_models import DBPost, DBCollectionTask
from big5_databases.databases.model_conversion import PostModel, CollectionTaskModel


def get_tasks_with_posts(db: "DatabaseManager", platform: str) -> Generator[
    tuple[CollectionTaskModel, list[PostModel]], None, None]:
    """Get all collection tasks with their associated posts from a database."""
    with db.get_session() as session:
        # First get all tasks
        tasks_query = select(DBCollectionTask).where(DBCollectionTask.platform == platform)
        tasks = session.execute(tasks_query).scalars()

        for task in tasks:
            # For each task, get its associated posts
            posts_query = select(DBPost).where(DBPost.collection_task_id == task.id)
            posts = session.execute(posts_query).scalars()

            # Convert both task and posts to their models
            yield task.model(), [post.model() for post in posts]


def fix_db(source_db_path: Path, target_db_path: Path, platform: str) -> MergeStats:
    """
    Merge one database/platform into another.

    Args:
        source_db_path: Path to the source database
        target_db_path: Path to the target database
        platform: Platform name (e.g., "youtube", "tiktok")

    Returns:
        MergeStats: Statistics about the merge operation
    """
    # Initialize database managers for source and target
    source_db = DatabaseManager.sqlite_db_from_path(source_db_path, False)
    target_db = DatabaseManager.sqlite_db_from_path(target_db_path, True)
    stats = MergeStats()

    tasks_to_delete: list[int] = []
    posts_to_delete: list[int] = []

    batch_size = 500
    # Open a session with the target database
    with target_db.get_session() as target_session:
        # Process each collection task and its posts from the source
        for task_model, posts_models in get_tasks_with_posts(source_db, platform):
            stats.total_posts_found += len(posts_models)
            tasks_to_delete.append(task_model.id)
            tasks_to_delete.extend([p.id for p in posts_models])
            # Check which posts already exist in the target
            new_posts = filter_posts_with_existing_post_ids(posts_models, session=target_session)
            stats.duplicated_posts_skipped += len(posts_models) - len(new_posts)
            stats.new_posts_added += len(new_posts)

            # Handle the collection task (find existing or create new)
            target_task = process_collection_task(
                target_session,
                task_model,
                len(new_posts),
                stats
            )

            # Add the new posts to the target database
            for post in new_posts:
                # Set the collection task ID to the target task
                post.collection_task_id = target_task.id
                # Add metadata about the source database
                if hasattr(post, 'metadata_content') and post.metadata_content:
                    post.metadata_content.orig_db_conf = (source_db_path.as_posix(), post.collection_task_id)

                # Convert to a database model and add to session
                post_data = post.model_dump(exclude={"id"})
                new_post = DBPost(**post_data)
                target_session.add(new_post)

            # Commit after processing each task's posts
            if len(target_session.new) >= batch_size:
                target_session.commit()

    with source_db.get_session() as source_session:
        source_session.execute(delete(DBPost).where(DBPost.id.in_(tasks_to_delete)))
        source_session.execute(delete(DBCollectionTask).where(DBCollectionTask.id.in_(tasks_to_delete)))

    return stats


if __name__ == "__main__":
    fix_db(Path("/home/rsoleyma/projects/platforms-clients/data/col_db/twitter/twitter.sqlite"),
           Path("/home/rsoleyma/projects/platforms-clients/data/col_db/youtube/from_twitter_db.sqlite"),
           "youtube")
