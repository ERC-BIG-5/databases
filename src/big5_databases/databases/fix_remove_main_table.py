from pathlib import Path

from sqlalchemy import create_engine

from big5_databases.databases.db_models import DBPlatformDatabase
from big5_databases.databases.external import SQliteConnection


def fix_db(db_path: Path) -> None:
    engine = create_engine(SQliteConnection(db_path=db_path).connection_str)
    DBPlatformDatabase.__table__.drop(engine, checkfirst=True)

if __name__ == '__main__':
    fix_db(Path(""))