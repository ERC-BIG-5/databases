from typing import Optional

from pydantic import Fieldº
from pydantic_settings import BaseSettings, SettingsConfigDict

from databases.external import ENV_FILE_PATH, BASE_DATA_PATH
from pydantic import field_validator, ValidationInfo

class PostgresCredentials(BaseSettings):
    model_config = SettingsConfigDict(env_file=ENV_FILE_PATH, env_file_encoding='utf-8', extra='allow')
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: str = "5432"
    DB_NAME: str = "big5"

    @property
    def connection_str(self) -> str:
        return (f"postgresql+psycopg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
                f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.DB_NAME}")


class SqliteSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=ENV_FILE_PATH, env_file_encoding='utf-8', extra='allow')
    DB_REL_PATH: Optional[str] = "db.sqlite"
    SQLITE_FILE_PATH: str = Field((BASE_DATA_PATH / DB_REL_PATH).absolute().as_posix())

    @field_validator("SQLITE_FILE_PATH")
    def set_sqlite_path(cls, v, values:ValidationInfo):
        return (BASE_DATA_PATH / values.data["DB_REL_PATH"]).absolute().as_posix()
