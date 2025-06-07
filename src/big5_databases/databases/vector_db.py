

import lancedb
import pyarrow as pa
from lancedb.db import DBConnection

from big5_databases.databases.external import LanceConnection


def setup(connection: LanceConnection) -> DBConnection:
    db = lancedb.connect(connection.db_path)
    schema = pa.schema([pa.field("vector", pa.list_(pa.float32(), list_size=2))])
    tbl = db.create_table("empty_table_async", schema=schema)
    return db


#db = DatabaseManager(DBConfig(db_connection=LanceConnection(db_path="vector-db")))
db = setup(LanceConnection(db_path="vector_db"))