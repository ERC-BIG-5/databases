from pathlib import Path
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine

from databases.db_models import DBPlatformDatabase, DBPlatformDatabase2
from databases.external import SQliteConnection


def fix_db(db_path: Path) -> None:
    engine = create_engine(SQliteConnection(db_path=db_path).connection_str)
    DBPlatformDatabase.__table__.drop(engine, checkfirst=True)
    DBPlatformDatabase2.__table__.drop(engine, checkfirst=True)

if __name__ == '__main__':
    fix_db(Path("/home/rsoleyma/projects/platforms-clients/data/col_db/archive/from_twitter_db.sqlite"))