"""
Database repair and validation utilities.

This module provides functions for validating and fixing database integrity issues,
particularly focusing on media file consistency between database records and
filesystem storage. It helps identify orphaned files and missing media references.
"""
from pathlib import Path

from tqdm import tqdm

from big5_databases.databases.db_mgmt import DatabaseManager
from big5_databases.databases.db_models import DBPost
from big5_databases.databases.model_conversion import PostMetadataModel


def check_media_files(db: DatabaseManager, media_folders: list[Path]) -> tuple[list[Path], list[str]]:
    """
    Check consistency between database media records and filesystem media files.

    This function validates that media files referenced in database records
    actually exist on the filesystem, and identifies orphaned files that
    exist in media folders but are not properly referenced in the database.

    Parameters
    ----------
    db : DatabaseManager
        Database manager instance for accessing post records.
    media_folders : list[Path]
        List of directories containing media files to validate.

    Returns
    -------
    tuple[list[Path], list[str]]
        A tuple containing:
        - list[Path]: Complete paths to orphaned media files that exist on
          filesystem but are not properly linked to database records
        - list[str]: Platform IDs of posts with missing media files that
          are referenced in database but don't exist on filesystem

    Notes
    -----
    This function performs several validation checks:

    1. **Orphan Detection**: Identifies media files in folders that belong to
       posts marked as failed downloads or with no media paths in database
    2. **Missing File Detection**: Finds database records that reference
       media files that don't exist on the filesystem
    3. **Batch Processing**: Uses yield_per for memory-efficient processing
       of large databases

    The function expects media files to be named with platform_id as prefix
    (e.g., "7237785209738005762_video.mp4") and uses this convention to
    match files to database records.

    Warning
    -------
    This function contains hardcoded test assertions and debug prints that
    should be removed for production use. The current implementation is
    designed for development and testing purposes.

    Todo
    ----
    - Remove hardcoded test platform_id checks
    - Improve multi-file validation logic
    - Add support for different file naming conventions
    - Implement proper path validation
    """
    remaining_file_pids = [
        [file.name.split("_")[0] for file in media_folder.iterdir()] for media_folder in media_folders
    ]
    missing2: list[str] = []

    # # todo, out,  TEST
    assert "7237785209738005762" in remaining_file_pids[0]

    # files but not marked in the db, base_path and pid
    orphan_files: list[tuple[Path, str]] = []

    with db.get_session() as session:
        pbar = tqdm()
        query = session.query(DBPost.id, DBPost.platform_id, DBPost.metadata_content).yield_per(200)
        for (id, pid, metadata) in query:
            pbar.update(1)
            paths, base, failed = tuple(metadata.get(k)
                                        for k in ["media_paths", "media_base_path", "media_dl_failed"])
            # todo, out, TEST
            if pid == "7237785209738005762":
                print(paths, base, failed)

            # check if we actually have it
            if failed or not paths:
                for f_idx, mf in enumerate(remaining_file_pids):
                    if pid in mf:
                        orphan_files.append((media_folders[f_idx], pid))
            else:
                m = PostMetadataModel.model_construct(media_paths=paths, media_base_path=base, media_dl_failed=failed)
                full_paths = m.mediafile_paths
                for fp in full_paths:
                    if not fp.exists():
                        missing2.append(pid)

                # mf_found = False
                # for mf in remaining_file_pids:
                #     if pid in mf:
                #         mf_found = True
                #         mf.remove(pid)
                #         break

    complete_orphan_files: list[Path] = []
    for base_p, pid in orphan_files:
        complete_orphan_files.extend(list(base_p.glob(f"{pid}*.*")))

    """
    complete_missing_files : list[str] = []
    for folder_files in remaining_file_pids:
        complete_missing_files.extend(folder_files)
    """

    return complete_orphan_files, missing2  # complete_missing_files

def fix_media_files(orphan_files: list[Path], missing_files: list[str]):
    """
    Fix media file inconsistencies identified by check_media_files.

    Parameters
    ----------
    orphan_files : list[Path]
        List of orphaned media file paths to process.
    missing_files : list[str]
        List of platform IDs with missing media files.

    Notes
    -----
    This function is not yet implemented. It should handle:
    - Moving or linking orphaned files to correct database records
    - Updating database metadata for missing files
    - Cleaning up invalid media references

    Todo
    ----
    Implementation needed for media file repair operations.
    """
    pass
