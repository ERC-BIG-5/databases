# DEPRECATED: This file is kept for backward compatibility only.
# Functions have been moved to specialized modules:
# - db_operations.py - core database operations (filtering, task management)
# - db_analytics.py - analytics and reporting functions
#
# Please update imports to use the new consolidated modules.

import os
from pathlib import Path
from typing import Union
from big5_databases.databases.external import SQliteConnection
from big5_databases.databases.model_conversion import PlatformDatabaseModel


# File system utilities - these should be moved to a separate filesystem utilities module
# Kept here temporarily for compatibility

def file_size(db: Union["DatabaseManager", PlatformDatabaseModel]) -> int:
    """Get database file size in bytes. DEPRECATED: Use DatabaseManager._file_size() instead."""
    if isinstance(db, PlatformDatabaseModel):
        file_path = db.full_path
    elif hasattr(db, 'config') and isinstance(db.config.db_connection, SQliteConnection):
        file_path = db.config.db_connection.db_path
    else:
        return 0
    return os.stat(file_path).st_size

def file_modified(db: Union["DatabaseManager", PlatformDatabaseModel]) -> float:
    """Get database file modification timestamp. DEPRECATED: Use DatabaseManager._file_modified() instead."""
    if isinstance(db, PlatformDatabaseModel):
        file_path = db.full_path
    elif hasattr(db, 'config') and isinstance(db.config.db_connection, SQliteConnection):
        file_path = db.config.db_connection.db_path
    else:
        return 0
    return file_path.stat().st_mtime

def currently_open(db: Union["DatabaseManager", PlatformDatabaseModel]) -> bool:
    """Check if database is currently open. DEPRECATED: Use DatabaseManager._currently_open() instead."""
    if isinstance(db, PlatformDatabaseModel):
        file_path = db.full_path
    elif hasattr(db, 'config') and isinstance(db.config.db_connection, SQliteConnection):
        file_path = db.config.db_connection.db_path
    else:
        return False
    wal_path = str(file_path) + '-wal'
    return os.path.exists(wal_path) and os.path.getsize(wal_path) > 0