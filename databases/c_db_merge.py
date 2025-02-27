from pathlib import Path
from typing import Dict, Any
from dataclasses import dataclass

from tqdm import tqdm
from sqlalchemy import select
from sqlalchemy.orm import Session

from databases.db_mgmt import DatabaseManager
from databases.db_models import DBPost, DBCollectionTask
from databases.db_utils import get_tasks_with_posts, filter_posts_with_existing_post_ids
from tools.env_root import root


@dataclass
class MergeStats:
    """Statistics about the merge operation."""
    total_posts_found: int = 0
    duplicated_posts_skipped: int = 0
    new_posts_added: int = 0
    existing_tasks_updated: int = 0
    new_tasks_created: int = 0

    def __str__(self) -> str:
        return (
            f"Merge Statistics:\n"
            f"  Total posts found: {self.total_posts_found}\n"
            f"  Duplicated posts skipped: {self.duplicated_posts_skipped}\n"
            f"  New posts added: {self.new_posts_added}\n"
            f"  Existing tasks updated: {self.existing_tasks_updated}\n"
            f"  New tasks created: {self.new_tasks_created}"
        )


def merge_database(source_db_path: Path, target_db_path: Path) -> MergeStats:
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
    target_db = DatabaseManager.sqlite_db_from_path(target_db_path, False)
    stats = MergeStats()

    batch_size = 500
    # Open a session with the target database
    with target_db.get_session() as target_session:
        # Process each collection task and its posts from the source
        for task_model, posts_models in tqdm(get_tasks_with_posts(source_db)):
            stats.total_posts_found += len(posts_models)

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

    return stats


def process_collection_task(session: Session, task_model, num_new_posts: int, stats: MergeStats):
    """Process a collection task - find existing or create new in target database."""
    # Check if task already exists by task_name
    existing_task = session.execute(
        select(DBCollectionTask).where(DBCollectionTask.task_name == task_model.task_name)
    ).scalar()

    if existing_task:
        # Update the existing task with the new post counts
        existing_task.found_items = (getattr(existing_task,"found_items") or 0) + num_new_posts
        existing_task.added_items = (getattr(existing_task, "added_items") or 0) + num_new_posts
        stats.existing_tasks_updated += 1
        return existing_task
    else:
        # Create a new task
        new_task = DBCollectionTask(**task_model.model_dump(exclude={"id"}))
        new_task.found_items = num_new_posts
        new_task.added_items = num_new_posts
        session.add(new_task)
        session.flush()  # Generate an ID for the new task
        stats.new_tasks_created += 1
        return new_task


def check_for_conflicts(source_db_path: Path, target_db_path: Path) -> Dict[str, Any]:
    """
    Check for conflicts between source and target databases.

    Returns a dictionary with:
    - count of potentially conflicting posts
    - database sizes
    - details of conflicts if needed
    """
    source_db = DatabaseManager.sqlite_db_from_path(source_db_path, False)
    target_db = DatabaseManager.sqlite_db_from_path(target_db_path, False)

    source_posts = {}
    target_posts = {}

    # Collect all posts from source
    with source_db.get_session() as session:
        posts_query = select(DBPost)
        for post in session.execute(posts_query).scalars():
            post_model = post.model()
            source_posts[post_model.platform_id] = True

    # Collect all posts from target
    with target_db.get_session() as session:
        posts_query = select(DBPost)
        for post in session.execute(posts_query).scalars():
            post_model = post.model()
            target_posts[post_model.platform_id] = True

    # Find conflicts
    conflicts = set(source_posts.keys()) & set(target_posts.keys())

    return {
        "source_size": len(source_posts),
        "target_size": len(target_posts),
        "conflicts": len(conflicts),
        "conflict_percentage": len(conflicts) / len(source_posts) * 100 if source_posts else 0
    }


# Example usage:
if __name__ == "__main__":
    root("/home/rsoleyma/projects/platforms-clients")
    source_group = [
        # "data/col_db/tiktok/rm/tiktok_alt.sqlite",
        "data/col_db/tiktok/rm/tiktok.sqlite",
        # "/home/rsoleyma/projects/platforms-clients/data/youtube2024.sqlite",
        # "/home/rsoleyma/projects/platforms-clients/data/db_safe.sqlite",
        # "/home/rsoleyma/projects/platforms-clients/data/youtube_merged.sqlite",
        # "/home/rsoleyma/projects/platforms-clients/data/col_db/youtube/from_twitter_db.sqlite"
    ]
    for source in source_group:
        source_path = Path(source).absolute()
        print(source_path.relative_to(root()))
        if not source_path.exists():
            continue

        target_path = root() / "data/tiktok.sqlite"

        # Optional: Check for conflicts first
        # conflicts = check_for_conflicts(source_path, target_path)
        # print(f"Potential conflicts: {conflicts['conflicts']} posts ({conflicts['conflict_percentage']:.2f}%)")

        # Perform the merge
        stats = merge_database(source_path, target_path)
        print(stats)
