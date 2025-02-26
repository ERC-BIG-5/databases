import math
from datetime import datetime
from enum import Enum, auto
from pathlib import Path
from pydantic import BaseModel, Field, computed_field, SecretStr, field_serializer
from typing import Optional, Literal

from tools.env_root import root

BASE_DATA_PATH = root() / "data"
ENV_FILE_PATH = Path(".env")


class PostType(Enum):
    REGULAR = auto()


DatabaseType = Literal["sqlite", "postgres"]
type DatabaseConnectionType = SQliteConnection | PostgresConnection


class CollectionStatus(Enum):
    INIT = auto()
    RUNNING = auto()  # started and currently running
    PAUSED = auto()  # if it's set to pause
    ABORTED = auto()  # started and aborted
    DONE = auto()  # started and finished


class SQliteConnection(BaseModel):
    db_path: Path
    reset_db: Optional[bool] = False

    @property
    def connection_str(self) -> str:
        if self.db_path.is_absolute():
            return f"sqlite:///{self.db_path}"
        else:
            return f"sqlite:///{(BASE_DATA_PATH / self.db_path).as_posix()}"


class PostgresConnection(BaseModel):
    name: str
    user: str
    password: SecretStr
    host: str
    port: int = 5432

    @property
    def connection_str(self) -> str:
        return (f"postgresql+psycopg://{self.user}:{self.password.get_secret_value()}@"
                f"{self.host}:{self.port}/{self.name}")


class DBConfig(BaseModel):
    model_config = {'extra': "forbid", "from_attributes": True}
    db_connection: DatabaseConnectionType
    create: bool = True
    # is_default: bool = Field(False)
    reset_db: bool = False
    test_mode: bool = False
    require_existing_parent_dir: Optional[bool] = Field(True,
                                                        description="SQLITE: When the db is created, it requires an existing parent directory.")
    tables: Optional[list[str]] = Field(default_factory=list)

    @computed_field
    @property
    def connection_str(self) -> str:
        return self.db_connection.connection_str

    @computed_field
    @property
    def db_type(self) -> DatabaseType:
        return "sqlite" if isinstance(self.db_connection, SQliteConnection) else "postgres"


class ClientConfig(BaseModel):
    model_config = {'extra': "forbid", "from_attributes": True}
    auth_config: Optional[dict[str, str]] = None
    request_delay: Optional[int] = 0
    delay_randomize: Optional[int] = 0
    progress: bool = True
    db_config: Optional[DBConfig] = None


class CollectConfig(BaseModel):
    model_config = {'extra': "allow"}
    query: Optional[str | dict] = ""
    limit: Optional[int] = 10000  # math.inf
    from_time: Optional[str] = None
    to_time: Optional[str] = None
    language: Optional[str] = None
    location_base: Optional[str] = None
    location_mod: Optional[str] = None


# todo, we still have something in the client
class ClientTaskConfig(BaseModel):
    model_config = {'extra': "forbid", "from_attributes": True}
    id: Optional[int] = Field(None, init=False)
    task_name: str
    platform: str
    database: Optional[str] = None  # default the same as platform
    collection_config: CollectConfig
    client_config: Optional[ClientConfig] = Field(default_factory=ClientConfig)
    transient: bool = False  # will be deleted after done
    source_file: Optional[Path] = None
    #
    test: bool = False
    overwrite: bool = False
    test_data: Optional[list[dict]] = None
    #
    status: CollectionStatus = Field(CollectionStatus.INIT, init=False)
    time_added: Optional[datetime] = Field(None, init=False)

    @field_serializer("status")
    def serialize_status(self, value: CollectionStatus) -> int:
        return value.value

    def __repr__(self):
        return f"Collection-Task: {self.task_name} ({self.platform})"
