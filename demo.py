from databases.db_mgmt import DatabaseManager
from databases.external import DBConfig, SQliteConnection

# will not work, because field create is by default false
try:
    db = DatabaseManager(DBConfig(db_connection=SQliteConnection(db_path="database.sqlite")))
except ValueError as e:
    print("create not set")
    print(e)

try:
    db = DatabaseManager(DBConfig(db_connection=SQliteConnection(db_path="database.sqlite"),
                                  create=True))
except ValueError as e:
    print("require_existing_parent_dir is still True")
    print(e)


db = DatabaseManager(DBConfig(db_connection=SQliteConnection(db_path="database.sqlite"),
                              create=True,
                              require_existing_parent_dir=False))