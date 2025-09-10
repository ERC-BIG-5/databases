from itertools import batched
from pathlib import Path
from typing import Callable

from big5_databases.databases.db_mgmt import DatabaseManager
from big5_databases.databases.db_models import DBPost, DBPostProcessItem
from big5_databases.databases.external import DBConfig, SQliteConnection
from big5_databases.databases.meta_database import MetaDatabase
from big5_databases.databases.model_conversion import PostModel

TEMP_MAIN_DB = "/home/rsoleyma/projects/big5/platform_clients/data/dbs/main.sqlite"
BATCH_SIZE = 200

def post_text(p: PostModel) -> list[str]:
    match p.platform:
        case "youtube":
            return [p.content["snippet"]["title"],p.content["snippet"]["description"]]
        case _:
            raise ValueError(f"unknown platform: {p.platform}")


def create_from_db(db_name: str, target_db: Path, input_data_method: Callable[[PostModel], dict | list]):
    if TEMP_MAIN_DB:
        db = MetaDatabase(Path(TEMP_MAIN_DB)).get(db_name)
    else:
        db = MetaDatabase().get(db_name)

    mgmt = db.get_mgmt()

    target_db = DatabaseManager(DBConfig(name=db_name,
                                         create=True,
                                         require_existing_parent_dir=False,
                                         tables=["ppitem"],
                                         db_connection=SQliteConnection(db_path=target_db)))

    with mgmt.get_session() as session:
        # todo, maybe just, "content", metadata_content"
        for batch in batched(session.query(DBPost).yield_per(BATCH_SIZE), BATCH_SIZE):
            batch_data = [(p.platform_id, input_data_method(p.model())) for p in batch]
            with target_db.get_session() as t_session:
                # todo, filter existing...
                t_session.bulk_save_objects([DBPostProcessItem(platform_id=p[0],
                                                                           input=p[1]) for p in batch_data])


if __name__ == "__main__":
    def text_fct(m: PostModel) -> list[str]:
        return post_text(m)


    name = "phase-2_youtube_es"
    create_from_db(name, Path(f"ana/{name}.sqlite"), text_fct)
