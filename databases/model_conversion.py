import math
from datetime import datetime
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field

from databases.external import CollectionStatus, PostType, CollectConfig


# Base Models
class BaseDBModel(BaseModel):
    """Base model with common fields"""
    id: int

    class Config:
        from_attributes = True
        validate_assignment = True


# Platform Models
class PlatformDatabaseModel(BaseDBModel):
    """Model for platform database configuration"""
    platform: str
    connection_str: str


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


class PostMetadataModel(BaseModel):
    class Config:
        validate_assignment = True

    media_paths: Optional[list[str]] = None
    media_base_path: Optional[str] = None
    media_dl_failed: Optional[bool] = None
    post_exists: Optional[bool] = None
    labels: Optional[list[str]] = None
    resolved_urls: Optional[dict] = None  # url_resolve_method
    language: Optional[dict[str, LanguageDetectionModel]] = Field(None, description="language_detection_method")

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
    date_created: datetime
    post_type: PostType
    content: dict
    metadata_content: Optional[PostMetadataModel] = Field(default_factory=PostMetadataModel)
    date_collected: datetime
    collection_task_id: Optional[int]
    comments: list[CommentModel] = Field(default_factory=list)

    @property
    def metadata_content_model(self):
        return PostMetadataModel.model_validate(self.metadata_content or {})

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
        raise ValueError(f"No get_platform_text defined for platform {self.platform}")


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
    transient: bool
