"""
Vector database management module for handling LanceDB operations.

This module provides a high-level interface for working with LanceDB vector databases,
including table management and data operations for similarity search and vector storage.

Requires
--------
This module requires the 'vector' optional dependency:
    `uv add --optional vector` or `uv add big5-databases[vector]`
"""
from typing import Any

try:
    import lancedb
    from lancedb._lancedb import Table
except ImportError:
    print(f"""In order to use VectorDB install the extra 'vector' for big5-databases.
    `uv add --optional vector` or
    `uv add big5-databases[vector]`""")

from big5_databases.databases.external import LanceConnection


class VectorDBManager:
    """
    Manager for LanceDB vector database operations.

    This class provides a high-level interface for managing LanceDB vector databases,
    handling table operations, and performing vector similarity searches. It manages
    connections to LanceDB instances and provides methods for data insertion and retrieval.

    Parameters
    ----------
    connection : LanceConnection
        Connection configuration for the LanceDB database including path and table schemas.

    Attributes
    ----------
    db : lancedb.DBConnection
        Active connection to the LanceDB database.
    tables : dict[str, Table]
        Cache of opened LanceDB tables for efficient access.

    Notes
    -----
    LanceDB is a columnar vector database optimized for machine learning workloads.
    It supports efficient similarity search and is particularly useful for embedding
    storage and retrieval in AI applications.
    """

    def __init__(self, connection: LanceConnection):
        """
        Initialize the VectorDBManager with a LanceDB connection.

        Parameters
        ----------
        connection : LanceConnection
            Connection configuration containing database path and table definitions.

        Notes
        -----
        Creates a connection to the LanceDB database at the specified path and
        initializes an empty table cache for efficient table access.
        """
        self.db = lancedb.connect(connection.db_path)
        self.tables = {}
        # for table,table_model in connection.tables.items():
        #     if table not in self.db.table_names():
        #         self.db.create_table(table.name, schema=table_model)

    def get_table(self, table_name: str) -> Table:
        """
        Get a LanceDB table by name with caching.

        Parameters
        ----------
        table_name : str
            Name of the table to retrieve.

        Returns
        -------
        lancedb._lancedb.Table
            The requested LanceDB table object.

        Raises
        ------
        ValueError
            If the specified table does not exist in the database.

        Notes
        -----
        Tables are cached after first access to improve performance. If the table
        doesn't exist in the database, an error is raised rather than creating it.
        """
        if table_name not in self.tables:
            if table_name not in self.db.table_names():
                raise ValueError(f"Unknown table '{table_name}'")
            self.tables[table_name] = self.db.open_table(table_name)
        return self.tables[table_name]

    def add_data(self, table: str, data: list[dict[str, Any]]) -> None:
        """
        Add data to a specified table in the vector database.

        Parameters
        ----------
        table : str
            Name of the table to add data to.
        data : list[dict[str, Any]]
            List of dictionaries containing the data to insert. Each dictionary
            represents a row with column names as keys and values as data.

        Notes
        -----
        Data is appended to the existing table using LanceDB's append mode.
        The table must already exist in the database. The data structure should
        match the table's schema for successful insertion.

        The method uses the 'append' mode which is efficient for adding new data
        without modifying existing records.
        """
        # assert "version" in data
        self.get_table(table).add(data, "append")
