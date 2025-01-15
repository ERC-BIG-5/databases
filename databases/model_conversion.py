from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

from databases.external import CollectionStatus, PostType


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

class PostMetadataModel(BaseModel):
    class Config:
        validate_assignment = True

    media_paths: Optional[list[str]] = None
    media_base_path: Optional[str] = None
    media_dl_failed: Optional[bool] = None
    post_exists: Optional[bool] = None


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


# Task Models
class CollectionTaskModel(BaseDBModel):
    """Model for collection tasks"""
    task_name: str
    platform: str
    collection_config: dict
    found_items: Optional[int]
    added_items: Optional[int]
    collection_duration: Optional[int]
    status: CollectionStatus
    time_added: datetime
    transient: bool

