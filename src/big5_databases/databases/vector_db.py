from typing import Any

import lancedb
from lancedb._lancedb import Table

from big5_databases.databases.external import LanceConnection


class VectorDBManager:

    def __init__(self, connection: LanceConnection):
        self.db = lancedb.connect(connection.db_path)
        self.tables = {}
        # for table,table_model in connection.tables.items():
        #     if table not in self.db.table_names():
        #         self.db.create_table(table.name, schema=table_model)

    def get_table(self, table_name: str) -> Table:
        if table_name not in self.tables:
            if table_name not in self.db.table_names():
                raise ValueError(f"Unknown table '{table_name}'")
            self.tables[table_name] = self.db.open_table(table_name)
        return self.tables[table_name]

    def add_data(self,table:str, data: list[dict[str,Any]]) -> None:
        # assert "version" in data
        self.get_table(table).add(data)
        
# db = DatabaseManager(DBConfig(db_connection=LanceConnection(db_path="vector-db")))
# db = VectorDBManager(LanceConnection(db_path="/home/rsoleyma/projects/big5/big5_databases/data/vector_db", create_tables=[
#     LanceTable(name="test",type=pa.float32(),size=1024)
# ]))
