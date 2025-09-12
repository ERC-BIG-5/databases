from pathlib import Path
from typing import Dict, Any
from dataclasses import dataclass

from tqdm import tqdm
from sqlalchemy import select
from sqlalchemy.orm import Session

from big5_databases.databases.db_operations import filter_posts_with_existing_post_ids, get_tasks_with_posts
from big5_databases.databases.meta_database import MetaDatabase
from big5_databases.databases.model_conversion import CollectionTaskModel
from .db_mgmt import DatabaseManager
from .db_models import DBPost, DBCollectionTask
from .external import CollectionStatus
from deepdiff import DeepHash

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


def get_db(db_path_or_name: Path | str) -> DatabaseManager:
    if isinstance(db_path_or_name, Path):
        return DatabaseManager.sqlite_db_from_path(db_path_or_name)
    else:  # if SETTINGS.main_db_path:
        return MetaDatabase().get_db_mgmt(db_path_or_name)


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
        existing_task.found_items = (getattr(existing_task, "found_items") or 0) + num_new_posts
        existing_task.added_items = (getattr(existing_task, "added_items") or 0) + num_new_posts
        stats.existing_tasks_updated += 1
        if task_model.status == CollectionStatus.DONE:
            existing_task.status = CollectionStatus.DONE
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


def check_for_conflicts(source: str | Path, target: str | Path) -> Dict[str, Any]:
    """
    Check for conflicts between source and target databases.

    Returns a dictionary with:
    - count of potentially conflicting posts
    - database sizes
    - details of conflicts if needed
    """
    source_db = get_db(source)
    target_db = get_db(target)

    print(f"{source_db.metadata.db_path=}")
    print(f"{target_db.metadata.db_path=}")

    with source_db.get_session() as session:
        source_tasks: set[str] = set(session.execute(select(DBCollectionTask.task_name)).scalars())

    with target_db.get_session() as session:
        target_tasks: set[str] = set(session.execute(select(DBCollectionTask.task_name)).scalars())

    # tasks that are in the source but not in the target
    # print(source_tasks - target_tasks)

    # compare_tasks = source_tasks - target_tasks

    # tasks that are in the source but not in the target
    existing = source_tasks & target_tasks

    with source_db.get_session() as session:
        f_source_tasks: list[CollectionTaskModel] = [t.model() for t in session.execute(select(DBCollectionTask).where(DBCollectionTask.task_name.in_(existing))).scalars().all()]

    with target_db.get_session() as session:
        f_target_tasks:  list[CollectionTaskModel] = [t.model() for t in session.execute(select(DBCollectionTask).where(DBCollectionTask.task_name.in_(existing))).scalars().all()]

    src_map = {t.task_name: t for t in f_source_tasks}
    tgt_map = {t.task_name: t for t in f_target_tasks}

    diff_status = 0
    diff_config = 0
    diff_both_init = 0
    diff_both_done = 0
    diff_both_other_status = 0
    # both done
    diff_fount_posts = 0
    # diff status
    source_done_: list[CollectionTaskModel] = []
    target_done_: list[CollectionTaskModel] = []
    diff_both_other_status_: list[tuple[str,CollectionStatus,CollectionStatus]] = []

    def update_task(from_db: DatabaseManager, to_db: DatabaseManager, from_task_model: CollectionTaskModel):
        with from_db.get_session() as session:
            new_posts = [p.model() for p in session.query(DBPost).filter(DBPost.collection_task_id == from_task_model.id).all()]

        new_posts_pids = [p.platform_id for p in new_posts]
        with to_db.get_session() as session:
            target_obj = session.query(DBCollectionTask).filter(DBCollectionTask.task_name == from_task_model.task_name).scalar()
            existing_posts = session.query(DBPost).filter(DBPost.platform_id.in_(new_posts_pids)).all()


    for t in existing:
        src = src_map[t]
        tgt = tgt_map[t]
        source_conf = src.collection_config
        target_conf = tgt.collection_config
        s_hash = DeepHash(source_conf)[source_conf]
        t_hash = DeepHash(target_conf)[target_conf]
        if s_hash == t_hash:
            if src.status == tgt.status:
                if src.status == CollectionStatus.INIT:
                    diff_both_init += 1
                elif src.status == CollectionStatus.DONE:
                    diff_both_done += 1
                    if src.found_items != tgt.found_items:
                        diff_fount_posts += 1
                else:
                    diff_both_other_status_.append((src.task_name, src.status,tgt.status))

            else:
                diff_status += 1
                if src.status == CollectionStatus.INIT and tgt.status == CollectionStatus.DONE :
                    target_done_.append(tgt)
                elif src.status == CollectionStatus.DONE and tgt.status == CollectionStatus.INIT:
                    source_done_.append(src)
                    update_task(source_db, target_db, src)
                else:
                    pass # other!
        else:
            print(f"Task {t} is different")
            diff_config += 1

    print("num match", len(existing))
    print("---- same hash")
    print("diff both init", diff_both_init)
    print("diff both done", diff_both_done)
    print("---")
    print(f"{diff_fount_posts=}")
    print("---")
    print("diff both other status", diff_both_other_status)
    print("diff status", diff_status)
    print("---")
    print(f"{len(source_done_)=}, {len(target_done_)=}")
    print("src done",[t.task_name for t in source_done_])
    print("target done",[t.task_name for t in target_done_])
    print(f"{diff_both_other_status_=}")
    print("---")
    print("------ diff hash")
    print("diff conf", diff_config)

# Example usage:
if __name__ == "__main__":
    check_for_conflicts(" phase-2_tiktok", "phase-2_vm_tiktok")
    # root("/home/rsoleyma/projects/platforms-clients")
    # source_group = [
    #     # "data/col_db/tiktok/rm/tiktok_alt.sqlite",
    #     "data/col_db/tiktok/rm/tiktok.sqlite",
    #     # "/home/rsoleyma/projects/platforms-clients/data/youtube2024.sqlite",
    #     # "/home/rsoleyma/projects/platforms-clients/data/db_safe.sqlite",
    #     # "/home/rsoleyma/projects/platforms-clients/data/youtube_merged.sqlite",
    #     # "/home/rsoleyma/projects/platforms-clients/data/col_db/youtube/from_twitter_db.sqlite"
    # ]
    # for source in source_group:
    #     source_path = Path(source).absolute()
    #     print(source_path.relative_to(root()))
    #     if not source_path.exists():
    #         continue
    #
    #     target_path = root() / "data/tiktok.sqlite"
    #
    #     # Optional: Check for conflicts first
    #     # conflicts = check_for_conflicts(source_path, target_path)
    #     # print(f"Potential conflicts: {conflicts['conflicts']} posts ({conflicts['conflict_percentage']:.2f}%)")
    #
    #     # Perform the merge
    #     stats = merge_database(source_path, target_path)
    #     print(stats)
