from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from tools.env_root import root

ENV_FILE_PATH = root() / ".env"


class PostgresCredentials(BaseSettings):
    """
    PostgreSQL database credentials and connection settings.

    This class manages PostgreSQL connection parameters loaded from environment
    variables, providing a connection string for database access.

    Attributes
    ----------
    POSTGRES_USER : str
        PostgreSQL username.
    POSTGRES_PASSWORD : str
        PostgreSQL password.
    POSTGRES_HOST : str, optional
        PostgreSQL host address, by default "localhost".
    POSTGRES_PORT : str, optional
        PostgreSQL port number, by default "5432".
    DB_NAME : str, optional
        Database name, by default "big5".
    """
    model_config = SettingsConfigDict(env_file=ENV_FILE_PATH, env_file_encoding='utf-8', extra='allow')
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: str = "5432"
    DB_NAME: str = "big5"

    @property
    def connection_str(self) -> str:
        """
        Generate PostgreSQL connection string.

        Returns
        -------
        str
            PostgreSQL connection string in the format:
            postgresql+psycopg://user:password@host:port/database
        """
        return (f"postgresql+psycopg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
                f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.DB_NAME}")


class SqliteSettings(BaseSettings):
    """
    SQLite database settings and file path configurations.

    This class manages SQLite-specific settings loaded from environment
    variables, including main database path and default base paths.

    Attributes
    ----------
    main_db_path : Optional[Path]
        Path to the main SQLite database file. Loaded from MAIN_DB_PATH environment variable.
    default_sqlite_dbs_base_path : Optional[Path]
        Base directory path for SQLite databases. Loaded from SQLITE_DBS_BASE_PATH
        environment variable, defaults to root()/"data"/"dbs".
    """
    model_config = SettingsConfigDict(env_file=ENV_FILE_PATH, env_file_encoding='utf-8', extra='allow')
    main_db_path: Optional[Path] = Field(None, alias="MAIN_DB_PATH")
    # todo, this does not seem to be used..?
    default_sqlite_dbs_base_path: Optional[Path] = Field((root() / "data" / "dbs"), alias="SQLITE_DBS_BASE_PATH")

class DatabaseSettings(BaseSettings):
    """
    General database settings for machine identification.

    This class manages general database configuration settings loaded
    from environment variables.

    Attributes
    ----------
    location : str
        Machine identifier for database location tracking.
    """
    model_config = SettingsConfigDict(env_file=ENV_FILE_PATH, env_file_encoding='utf-8', extra='allow')
    location: str = Field(description="machine identifier")

SETTINGS = SqliteSettings()
