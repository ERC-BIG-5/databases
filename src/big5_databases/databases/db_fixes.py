from pathlib import Path

from big5_databases.databases.db_mgmt import DatabaseManager
from big5_databases.databases.db_models import DBPost


def check_media_files(db: DatabaseManager, media_folders: list[Path]) -> None:
    """

    todo, this does not check multiple files, also does not check the right path
    basic check...
    """
    remaining_file_pids = [
        [file.name.split("_")[0] for file in media_folder.iterdir()] for media_folder in media_folders
    ]

    # files but not marked in the db
    not_in_db = []

    with db.get_session() as session:
        query = session.query(DBPost.id, DBPost.platform_id, DBPost.metadata_content).yield_per(200)
        for (id, pid, metadata) in query:
            paths, base, failed = tuple(metadata.get(k)
                                        for k in ["media_paths", "media_base_path", "media_dl_failed"])
            # check if we actually have it
            if failed or not paths:
                for mf in remaining_file_pids:
                    if pid in mf:
                        not_in_db.append(pid)
            else:
                mf_found = False
                for mf in remaining_file_pids:
                    if pid in mf:
                        mf_found = True
                        mf.remove(pid)
                        break

