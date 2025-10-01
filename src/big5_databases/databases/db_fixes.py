from pathlib import Path

from tqdm import tqdm

from big5_databases.databases.db_mgmt import DatabaseManager
from big5_databases.databases.db_models import DBPost


def check_media_files(db: DatabaseManager, media_folders: list[Path]) -> tuple[list[Path], list[str]]:
    """

    todo, this does not check multiple files, also does not check the right path
    basic check...
    """
    remaining_file_pids = [
        [file.name.split("_")[0] for file in media_folder.iterdir()] for media_folder in media_folders
    ]

    # files but not marked in the db, base_path and pid
    orphan_files: list[tuple[Path, str]] = []

    with db.get_session() as session:
        pbar = tqdm()
        query = session.query(DBPost.id, DBPost.platform_id, DBPost.metadata_content).yield_per(200)
        for (id, pid, metadata) in query:
            pbar.update(1)
            paths, base, failed = tuple(metadata.get(k)
                                        for k in ["media_paths", "media_base_path", "media_dl_failed"])
            # check if we actually have it
            if failed or not paths:
                for f_idx, mf in enumerate(remaining_file_pids):
                    if pid in mf:
                        orphan_files.append((media_folders[f_idx], pid))
            else:
                mf_found = False
                for mf in remaining_file_pids:
                    if pid in mf:
                        mf_found = True
                        mf.remove(pid)
                        break

    complete_orphan_files: list[Path] = []
    for base_p, pid in orphan_files:
        complete_orphan_files.extend(list(base_p.glob(f"{pid}*.*")))

    complete_missing_files : list[str] = []
    for folder_files in remaining_file_pids:
        complete_missing_files.extend(folder_files)

    return complete_orphan_files, complete_missing_files

    def fix_media_files(orphan_files: list[Path], missing_files):
        pass
