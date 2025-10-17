from collections import Counter
from datetime import date
from datetime import datetime
from enum import Enum, auto
from pathlib import Path
from typing import Optional, Literal, Annotated, Any

from pydantic import BaseModel
from pydantic import Field, computed_field, SecretStr, field_serializer
from pydantic import field_validator
from pydantic.functional_serializers import PlainSerializer
from tools.env_root import root
from tools.pydantic_annotated_types import SerializablePath, SerializableDatetime

from .db_settings import SqliteSettings

# todo kick this out
BASE_DATA_PATH = root() / "data"


class PostType(Enum):
    REGULAR = auto()


DatabaseType = Literal["sqlite", "postgres"]
type DatabaseConnectionType = SQliteConnection | PostgresConnection
type VectorDBConnectionType = LanceConnection


class CollectionStatus(Enum):
    INIT = auto()
    INVALID_CONF = auto()  # collection config is not valid
    RUNNING = auto()  # started and currently running
    PAUSED = auto()  # if it's set to pause
    ABORTED = auto()  # started and aborted
    DONE = auto()  # started and finished


class SQliteConnection(BaseModel):
    db_path: SerializablePath | str

    @field_validator("db_path", mode="before")
    def validate_path(cls, v) -> Path:
        path = Path(v)
        if not path.is_absolute():
            path = SqliteSettings().default_sqlite_dbs_base_path / path
        return path

    @property
    def connection_str(self) -> str:
        if self.db_path.is_absolute():
            return f"sqlite:///{self.db_path}"
        else:
            return f"sqlite:///{self.db_path.as_posix()}"


try:
    from lancedb.pydantic import LanceModel


    class LanceConnection(BaseModel):
        db_path: Path | str
        tables: dict[str, LanceModel] = Field(default_factory=dict)

        @field_validator("db_path", mode="before")
        def validate_path(cls, v) -> Path:
            path = Path(v)
            if not path.is_absolute():
                path = SqliteSettings().default_sqlite_dbs_base_path / path
            return path

except ImportError:
    pass


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
    model_config = {'extra': "allow", "from_attributes": True}
    name: Optional[str] = None
    db_connection: DatabaseConnectionType
    create: bool = False
    reset_db: bool = False
    test_mode: bool = False
    require_existing_parent_dir: Optional[bool] = Field(True,
                                                        description="SQLITE: When the db is created, it requires an existing parent directory.")
    tables: Optional[list[str]] = Field(default_factory=list)

    @property
    def connection_str(self) -> str:
        return self.db_connection.connection_str

    @property
    def db_type(self) -> DatabaseType:
        if isinstance(self.db_connection, SQliteConnection):
            return "sqlite"
        else:
            return "postgres"


class DBSetupConfig(DBConfig):
    name: str


class PlatformDBConfig(DBConfig):
    """Configuration for platform-specific databases with table type specification"""
    platform: str = Field(description="Platform name (e.g., 'tiktok', 'twitter', 'youtube')")
    table_type: Literal["posts", "process"] = Field(default="posts",
                                                    description="Type of database: posts (content storage) or process (task processing)")

    @property
    def platform_tables(self) -> list[str]:
        """Get constant tables based on table_type"""
        if self.table_type == "posts":
            return ["posts", "users", "collection_tasks"]
        else:  # process
            # todo wtf is process_status
            return ["collection_tasks", "process_status"]


class ClientConfig(BaseModel):
    model_config = {'extra': "ignore", "from_attributes": True}
    ignore_initial_quota_halt: Optional[bool] = Field(False, description="Ignore initial quota halt")
    request_delay: Optional[float] = Field(0, description="Wait-time after each task")
    delay_randomize: Optional[int] = Field(0, description="Additional random delay (0-`value`")
    max_tasks: Optional[int] = Field(None, description="Maximum number of tasks to run")
    progress: bool = Field(True, description="If platform should process tasks or not")
    max_tasks: Optional[int] = None


class ClientSetup(BaseModel):
    class Config:
        extra = "allow"
        from_attributes = True

    platform: str = Field(description="Platform name (e.g., 'tiktok', 'twitter', 'youtube')")
    client: Optional[ClientConfig] = None
    db: Optional[DBSetupConfig] = Field(None, description="Configuration of the database")


class CollectConfig(BaseModel):
    model_config = {'extra': "allow"}
    query: Optional[str | dict] = Field(None, description="Search query, or complex query object (e.g. for tiktok)")
    limit: Optional[int] = Field(None,
                                 description="max amount to collect (client might pass over this value with pagination, but will stop immediately)")
    from_time: Optional[str] = Field(None, description="start time filter")
    to_time: Optional[str] = Field(None, description="end time filter")
    language: Optional[str] = Field(None, description="language filter")
    location_base: Optional[str] = Field(None, description="location filter")
    location_mod: Optional[str] = Field(None, description="2nd location filter (e.g. radius)")


# todo, we still have something in the client
class ClientTaskConfig(BaseModel):
    model_config = {'extra': "forbid", "from_attributes": True}
    id: Optional[int] = Field(None, init=False)
    task_name: str = Field(description="unique name of the task")
    platform: str = Field(description="which social media platform")
    database_name: Optional[str] = Field(None, description="required when platform more is registered more than once")
    database: Optional[str] = Field(None, description="database name",
                                    deprecated=True)  # default the same as the platform
    collection_config: CollectConfig = Field(description="the actual collection configuration")
    platform_collection_config: Optional[dict] = None
    # client_config: Optional[ClientConfig] = Field(default_factory=ClientConfig, deprecated=True)
    transient: bool = Field(False, description="if the task should be deleted afterwards")

    #
    test: bool = False
    overwrite: bool = False
    keep_old_posts: bool = False  # if overwritten, the posts should be kept
    test_data: Optional[list[dict]] = None
    timestamp_submitted: Optional[
        SerializableDatetime] = None  # used already? when the config was commited/added to the db.
    #
    group_prefix: Optional[str] = None  # keep the group_prefix, in case we have 'force_new_index'  in the group
    force_new_index: Optional[bool] = False
    #
    status: CollectionStatus = Field(CollectionStatus.INIT)  # status of the task
    time_added: Optional[datetime] = Field(None)  # same as timestamp_submitted?

    @field_serializer("status")
    def serialize_status(self, status: CollectionStatus) -> int:
        return status.value

    def __repr__(self):
        return f"Collection-Task: {self.task_name} ({self.platform})"


def rel_path(p: Path) -> str:
    data_path = root() / "data"
    if p.is_relative_to(data_path):
        return p.relative_to(data_path).as_posix()
    else:
        return p.absolute().as_posix()


SerializablePath = Annotated[
    Path, PlainSerializer(rel_path, return_type=str)
]

AbsSerializablePath = Annotated[
    Path, PlainSerializer(lambda p: str(p.absolute()), return_type=str, when_used="always")
]


class RawStats(BaseModel):
    """Simple statistics model that stores counts by period string keys."""
    total_count: int = 0
    min_date: Optional[str] = None
    max_date: Optional[str] = None
    counter: Counter[str] = Counter()

    def add(self, period_str: str, count: int = 1) -> None:
        """Add a count for a specific period string."""
        self.total_count += count
        self.counter[period_str] += count

        # We're not dealing with actual date objects, but we can still track
        # min/max period strings lexicographically for reporting purposes
        if self.min_date is None or period_str < self.min_date:
            self.min_date = period_str
        if self.max_date is None or period_str > self.max_date:
            self.max_date = period_str

    def set(self, period_str: str, count: int) -> None:
        """Set the count for a specific period string."""
        self.total_count += count

        # Check if the period already exists in the counter
        if period_str in self.counter:
            print(f"Warning: {period_str} already exists in counter")
            return

        self.counter[period_str] = count

        # Update min/max date strings
        if self.min_date is None or period_str < self.min_date:
            self.min_date = period_str
        if self.max_date is None or period_str > self.max_date:
            self.max_date = period_str


class TimeWindow(str, Enum):
    DAY = "day"
    MONTH = "month"
    YEAR = "year"

    @property
    def time_str(self) -> str:
        match self:
            case TimeWindow.DAY:
                # Format as YYYY-MM-DD (year-month-day)
                return '%Y-%m-%d'
            case TimeWindow.MONTH:
                # Format as YYYY-MM (year-month)
                return '%Y-%m'
            case TimeWindow.YEAR:
                # Format as YYYY (year)
                return '%Y'
            case _:
                raise ValueError(f"Unsupported time window: {self}")


class TimeColumn(str, Enum):
    CREATED = "created"
    COLLECTED = "collected"


class DBStats(BaseModel):
    """Database statistics model with file information and error handling."""
    db_path: SerializablePath
    created_counts: RawStats = RawStats()
    collected_counts: RawStats = RawStats()
    period: Annotated[TimeWindow, PlainSerializer(lambda v: v.value, return_type=str,
                                                  when_used="always")]
    # time_column: Annotated[TimeColumn, PlainSerializer(lambda v: v.value, return_type=str,
    #                                               when_used="always")]
    error: Optional[str] = None
    file_size: int = 0

    @field_validator("db_path")
    def validate_db_path(cls, v):
        """Ensure db_path is absolute."""
        if not v.is_absolute():
            v = root() / "data" / v
        return v

    def plot_daily_items(self, bars: bool = False, period: TimeWindow = TimeWindow.DAY,
                         title: Optional[str] = "") -> "plt":
        try:
            import matplotlib.dates as mdates
            import matplotlib.pyplot as plt
            import pandas as pd
            import seaborn as sns
        except ModuleNotFoundError:
            print("You need to add the optional dependency 'plot'")
            return

        plt.figure(figsize=(12, 6))

        daily_counts = pd.Series(self.period_stats(period).counter)
        # Convert index to datetime if not already
        if not isinstance(daily_counts.index, pd.DatetimeIndex):
            daily_counts.index = pd.to_datetime(daily_counts.index)

        if bars:
            width = 1 if period is TimeWindow.DAY else 25
            plt.bar(daily_counts.index, daily_counts.values, width=width,
                    color='blue', label='Posts', alpha=0.7)
        else:
            sns.lineplot(data=daily_counts, color='blue', label='Posts')

        # Zero days highlight in red
        zero_days = daily_counts[daily_counts == 0]
        if not zero_days.empty:
            if bars:
                plt.bar(zero_days.index, zero_days.values,
                        color='red', label='No Posts',
                        zorder=5)
            else:
                plt.scatter(zero_days.index, zero_days.values,
                            color='red', s=10, label='No Posts',
                            zorder=5)

        plt.title(title)
        plt.xlabel('Date')
        plt.ylabel('Number of Posts')

        # Improved x-axis labels
        # plt.gca().xaxis.set_major_locator(mdates.MonthLocator())
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
        plt.xticks(rotation=45)

        plt.grid(True, alpha=0.3)
        # plt.legend()
        plt.tight_layout()
        return plt

    def period_stats(self, period: TimeWindow, col: TimeColumn) -> RawStats:
        stats = RawStats()
        cut_index = -0
        match period:
            case TimeWindow.DAY:
                pass
            case TimeWindow.MONTH:
                month_from_days = Counter()
                cut_index = 7
            case TimeWindow.YEAR:
                month_from_days = Counter()
                cut_index = 4

        counts = self.created_counts if col == TimeColumn.CREATED else self.collected_counts
        for day_key, count in counts.counter.items():
            stats.add(day_key[:cut_index], count)

        return stats

    def get_missing_days(self, start_date: date, end_date: date) -> list[date]:
        pass


class MetaDatabaseStatsModel(BaseModel):
    """Auto-calculated database statistics that can be safely overwritten"""
    model_config = {'extra': "forbid"}

    tasks_states: dict[str, int] = Field(default_factory=dict)
    post_count: int = 0
    file_size: int = 0
    last_modified: Optional[float] = None
    stats: Optional[DBStats] = Field(None)


class MetaDatabaseConfigModel(BaseModel):
    """Persistent user configuration that should be preserved"""
    model_config = {'extra': "allow"}

    annotation: Optional[str] = None
    config: Optional[ClientConfig] = None
    alternative_paths: Optional[dict[str, AbsSerializablePath]] = Field(default_factory=dict)


class MetaDatabaseContentModel(BaseModel):
    """Combined model for backward compatibility"""
    model_config = {'extra': "allow"}

    # Stats (auto-calculated)
    tasks_states: dict[str, int] = Field(default_factory=dict)
    post_count: int = 0
    file_size: int = 0
    last_modified: Optional[float] = None
    stats: Optional[DBStats] = Field(None)

    # Config (persistent)
    annotation: Optional[str] = None
    config: Optional[ClientConfig] = None
    client_setup: Optional["ClientSetup"] = None
    alternative_paths: Optional[dict[str, AbsSerializablePath]] = Field(default_factory=dict)
    run_states: Optional[list["DatabaseRunState"]] = Field(default_factory=list)

    def add_basestats(self, stats: "DatabaseBasestats") -> "MetaDatabaseContentModel":
        for k, v in stats.model_dump().items():
            setattr(self, k, v)
        return self


class DatabaseBasestats(BaseModel):
    model_config = {'extra': "forbid"}

    tasks_states: dict[str, int] = Field(default_factory=dict)
    post_count: int = 0
    file_size: int = 0
    last_modified: Optional[float] = None


class DatabaseRunState(BaseModel):
    pipeline_method: str
    location: str
    alt_db: Optional[str] = Field(None, description="Alternative database name. None, if its on the main db")

    @classmethod
    def from_stats_and_config(cls, stats: MetaDatabaseStatsModel,
                              config: MetaDatabaseConfigModel) -> "MetaDatabaseContentModel":
        """Create combined model from separate stats and config"""
        return cls(
            **stats.model_dump(),
            **config.model_dump()
        )

    def get_stats(self) -> MetaDatabaseStatsModel:
        """Extract stats portion"""
        return MetaDatabaseStatsModel(
            tasks_states=self.tasks_states,
            post_count=self.post_count,
            file_size=self.file_size,
            last_modified=self.last_modified,
            stats=self.stats
        )

    def get_config(self) -> MetaDatabaseConfigModel:
        """Extract config portion"""
        return MetaDatabaseConfigModel(
            annotation=self.annotation,
            config=self.config,
            alternative_paths=self.alternative_paths
        )
