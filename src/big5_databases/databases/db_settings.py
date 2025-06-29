from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from tools.env_root import root

ENV_FILE_PATH = root() / ".env"

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
    SQLITE_DBS_BASE_PATH: str = Field((root() / "data" / "dbs").absolute().as_posix())

    # @field_validator("model_config")
    # def set_sqlite_path(cls, v, values:ValidationInfo):
    #     return (BASE_DATA_PATH / values.data["DB_REL_PATH"]).absolute().as_posix()

