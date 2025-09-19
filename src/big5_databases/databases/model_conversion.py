from datetime import datetime
from pathlib import Path
from typing import Optional, Annotated, Any, TYPE_CHECKING

from deprecated.classic import deprecated
from pydantic import BaseModel, Field, field_validator, ConfigDict, PlainSerializer
from tools.project_logging import get_logger

from tools.pydantic_annotated_types import SerializableDatetimeAlways

from .db_settings import SqliteSettings
from .external import CollectionStatus, PostType, CollectConfig, MetaDatabaseContentModel, SerializablePath, \
    AbsSerializablePath

if TYPE_CHECKING:
    from .db_mgmt import DatabaseManager

logger = get_logger(__file__)


# Base Models
class BaseDBModel(BaseModel):
    """Base model with common fields"""
    id: int

    class Config:
        from_attributes = True
        validate_assignment = True


class PlatformDatabaseContentModel(BaseDBModel):
    status: dict
    stats: dict


# Platform Models

class PlatformDatabaseModel(BaseDBModel):
    """Model for platform database configuration"""
    platform: str
    name: Optional[str] = None
    db_path: AbsSerializablePath

    id: Optional[int] = None
    is_default: bool = False
    content: MetaDatabaseContentModel = Field(default_factory=MetaDatabaseContentModel)
    last_content_update: Optional[datetime] = None
    # these come from former class
    # connection_str: str
    last_status_update: Optional[datetime] = None
    last_stats_update: Optional[datetime] = None

    @property
    def full_path(self) -> Path:
        if not self.db_path.is_absolute():
            return SqliteSettings().default_sqlite_dbs_base_path / self.db_path
        return self.db_path

    def exists(self):
        return self.full_path.exists()

    # todo allow passing in the config
    def get_mgmt(self, meta_db: Optional["PlatformDatabaseModel"] = None) -> "DatabaseManager":
        if not self.exists():
            raise ValueError(f"Could not load database {self.db_path} from meta-database")
        from .db_mgmt import DatabaseManager
        mgmt = DatabaseManager.sqlite_db_from_path(self.db_path)
        mgmt.metadata = meta_db
        return mgmt


# User Models
class UserModel(BaseDBModel):
    """Model for user data"""
    platform: str
    platform_username: Optional[str] = None


# Comment Models
class CommentModel(BaseDBModel):
    """Model for post comments"""
    date_created: Optional[datetime]
    content: str
    date_collected: datetime
    post_id: int


class LanguageDetectionModel(BaseModel):
    label: str
    score: float
    error: Optional[str] = None


## url_resolve_method
class PostTextReplacementPart(BaseModel):
    model_config = ConfigDict(validate_assignment=True)
    resolved_text: Optional[str] = None
    replaced_text: Optional[str] = None
    resolved_urls: dict[str, Optional[str]] = Field(default_factory=dict)

    def get_replace_with(self, orig_text: str, replace_text: str) -> str:
        text = orig_text
        for orig in self.resolved_urls.keys():
            text = text.replace(orig, replace_text)
        return text

    def get_resolved_text(self, orig_text: str) -> str:
        text = orig_text
        for orig, repl in self.resolved_urls.items():
            if not repl:
                repl = orig
            text = text.replace(orig, repl)
        return text


class PostTextReplacement(BaseModel):
    parts: dict[str, PostTextReplacementPart] = Field(default_factory=dict)

    def get_all_replaced(self, orig_text, replace_text) -> dict[str, str]:
        return {
            k: part.get_replace_with(orig_text, replace_text)
            for k, part in self.parts.items()
        }

    def get_resolved_texts(self, orig_texts: dict[str, str]) -> dict[str, str]:
        return {
            k: part.get_resolved_text(orig_texts[k])
            for k, part in self.parts.items()
        }


#####

class PostMetadataModel(BaseModel):
    class Config:
        validate_assignment = True

    media_paths: Optional[list[str]] = None
    media_base_path: Optional[str] = None
    media_dl_failed: Optional[bool] = None

    post_exists: Optional[bool] = None
    labels: Optional[list[str]] = None
    resolved_urls: Optional[PostTextReplacement] = None  # url_resolve_method
    language: Optional[dict[str, LanguageDetectionModel]] = Field(None, description="language_detection_method")

    orig_db_conf: Optional[tuple[str, Optional[int]]] = Field(None,
                                                              description="original database_name, collection_task_id) for merges")
    annotations: Optional[dict[str, dict]] = Field(None, description="annotations from labelstudio")

    # platform specific info
    extra: Optional[dict[str, Any]] = Field(None, description="platform specific")

    # hash_id: Optional[str] # WEIBO

    @property
    def mediafile_paths(self) -> list[Path]:
        media_ps = self.media_paths or []
        if not media_ps:
            return []
        base = Path(self.media_base_path)
        return [base / p for p in media_ps]


# Post Models
class PostModel(BaseDBModel):
    """Model for posts from any platform"""
    platform: str
    platform_id: Optional[str]
    post_url: str
    date_created: SerializableDatetimeAlways
    post_type: Annotated[PostType, PlainSerializer(lambda t: t.value, return_type=int, when_used='always')]
    content: dict
    metadata_content: Optional[PostMetadataModel] = Field(default_factory=PostMetadataModel)
    collection_task_id: Optional[int]
    comments: list[CommentModel] = Field(default_factory=list)

    # todo, we need those, for when the db col is null
    @field_validator("metadata_content", mode="after")
    def validate_metadata_content(cls, value):
        if value is None:
            return PostMetadataModel()
        return value

    @property
    @deprecated(reason="just use metadata_content")
    def metadata_content_model(self) -> PostMetadataModel:
        return self.metadata_content

    def get_platform_text(self) -> dict[str, str]:
        """
        since some platforms have multiple texts, we have content.<key> : new_text dicts
        :param platform:
        :param post:
        :return:
        """
        if self.platform == "twitter":
            return {"rawContent": self.content["rawContent"]}
        elif self.platform == "youtube":
            return {"title": self.content["snippet"]["title"],
                    "description": self.content["snippet"]["description"]}
        elif self.platform == "tiktok":
            return {"description": self.content["video_description"]}

        raise ValueError(f"No get_platform_text defined for platform {self.platform}")

    def get_media_urls(self, config: Optional[str]) -> dict[str, list[str]]:
        if self.platform == "twitter":
            return {
                "photos": [p["url"] for p in self.content["media"].get("photos")],
                # "videos" :  [p["url"] for p in self.content["media"].get("videos")],
                # animated = item.content["media"].get("animated")
            }
        elif self.platform == "tiktok":
            base_url = "https://www.tiktok.com/@{username}/video/{video_id}"
        elif self.platform == "youtube":
            thumbnails = self.content.get("snippet", {}).get("thumbnails", {})
            if config not in thumbnails:
                logger.warning(f"youtube video '{self.platform_id}' has no key: '{config}' in thumbnails")
                return {}
            return {
                f"thumbnail": thumbnails[config]["url"],
            }
        else:
            raise NotImplemented(
                f"please implement a function that gets the media paths of data of platform {self.platform}")


# Task Models
class CollectionTaskModel(BaseDBModel):
    """Model for collection tasks"""
    task_name: str
    platform: str
    collection_config: CollectConfig
    found_items: Optional[int]
    added_items: Optional[int]
    collection_duration: Optional[int]
    status: CollectionStatus
    time_added: datetime
    transient: Optional[bool] = False


class PostProcessModel(BaseDBModel):
    platform_id: str
    input: dict
    output: dict = Field(None)
