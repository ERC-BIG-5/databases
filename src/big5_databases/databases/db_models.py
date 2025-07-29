from dataclasses import dataclass
from datetime import datetime
from typing import TypedDict, TypeVar, Generic

from pydantic import BaseModel
from sqlalchemy import Column, String, Integer, Boolean, DateTime, ForeignKey, JSON, Enum, func, UniqueConstraint
from sqlalchemy import Enum as SQLAlchemyEnum
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import relationship, Mapped, mapped_column, declarative_base

from tools.pydantic_annotated_types import SerializableDatetimeAlways
from .external import CollectionStatus, ClientTaskConfig
from .external import PostType
from .model_conversion import CollectionTaskModel, PostModel, PlatformDatabaseModel

Base = declarative_base()

T = TypeVar('T', bound=BaseModel)


class EmptyModel(BaseModel):
    pass


class DBModelBase(Generic[T], Base):
    """
    Generic base class for all database models that can be converted to Pydantic models
    """
    __abstract__ = True
    _pydantic_model: T

    def model(self) -> T:
        return self._pydantic_model.model_validate(self, from_attributes=True)


class DBUser(Base):
    __tablename__ = 'user'

    id: Mapped[int] = mapped_column(primary_key=True)
    platform: Mapped[str] = mapped_column(String(20), nullable=False)
    platform_username: Mapped[str] = mapped_column(String(20), nullable=True)

    # posts: Mapped[list["DBPost"]] = relationship(back_populates="user")


class DBComment(Base):
    """
    """
    __tablename__ = 'comment'
    id: Mapped[int] = mapped_column(primary_key=True)
    date_created: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    content: Mapped[str] = mapped_column(String(200), nullable=False)
    date_collected: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    post_id: Mapped[int] = mapped_column(ForeignKey("post.id"))
    post: Mapped["DBPost"] = relationship(back_populates="comments")


class DBCollectionTask(DBModelBase[CollectionTaskModel]):
    __tablename__ = 'collection_task'

    # this for alembic
    __table_args__ = (
        UniqueConstraint('task_name', name='uq_task_name'),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    task_name: Mapped[str] = mapped_column(String(50), nullable=False)
    platform: Mapped[str] = mapped_column(String(20), nullable=False)
    collection_config: Mapped[dict] = mapped_column(JSON, nullable=False)
    platform_collection_config: Mapped[dict] = mapped_column(JSON, nullable=True)
    found_items: Mapped[int] = mapped_column(Integer, nullable=True)
    added_items: Mapped[int] = mapped_column(Integer, nullable=True)

    collection_duration: Mapped[int] = mapped_column(Integer, nullable=True)  # in millis
    status: Mapped[CollectionStatus] = mapped_column(SQLAlchemyEnum(CollectionStatus), nullable=False,
                                                     default=CollectionStatus.INIT)
    transient: Mapped[bool] = mapped_column(Boolean, nullable=True, default=False)
    time_added: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    execution_ts: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    database: Mapped[str] = mapped_column(String(20), nullable=True)

    posts = relationship("DBPost", back_populates="collection_task",
                         cascade="all, delete, delete-orphan")

    def __repr__(self) -> str:
        return f"CollectionTask: '{self.task_name}' / {self.platform}. ({self.status.name})"

    _pydantic_model = CollectionTaskModel


class DBPost(DBModelBase[PostModel]):
    __tablename__ = 'post'

    # this for alembic
    __table_args__ = (
        UniqueConstraint('platform_id', name='uq_platform_id'),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    platform: Mapped[str] = mapped_column(String(20), nullable=False)
    platform_id: Mapped[str] = mapped_column(String(50), nullable=False, unique=True)
    date_created: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    content: Mapped[dict] = Column(JSON)
    post_url: Mapped[str] = mapped_column(String(60), nullable=True)
    post_type: Mapped[PostType] = mapped_column(Enum(PostType), nullable=False, default=PostType.REGULAR)
    #
    metadata_content: Mapped[dict] = Column(JSON, default=dict)

    # todo: temp nullable
    collection_task: Mapped["DBCollectionTask"] = relationship(back_populates="posts")
    collection_task_id: Mapped[int] = mapped_column(ForeignKey("collection_task.id", ondelete="CASCADE"), nullable=True)

    # user_id: Mapped[int] = mapped_column(ForeignKey("user.id"))
    # user: Mapped[DBUser] = relationship(back_populates="posts")

    # content_schema_id: Mapped[int] = mapped_column(ForeignKey("post_content_schema.id"), nullable=True)
    # content_schema: Mapped[DBPostContentSchema] = relationship(back_populates="posts")

    comments: Mapped[list[DBComment]] = relationship(back_populates="post")

    _pydantic_model = PostModel


# class DBPlatformDatabase(DBModelBase[PlatformDatabaseModel]):
#     __tablename__ = 'platform_databases'
#
#     id: Mapped[int] = mapped_column(primary_key=True)
#     platform: Mapped[str] = mapped_column(String(20), nullable=False, unique=True)
#     name: Mapped[str] = mapped_column(String(20), nullable=True)
#     connection_str: Mapped[str] = mapped_column(String(), nullable=False)
#     is_default: Mapped[bool] = mapped_column(Boolean())
#
#     _pydantic_model = PlatformDatabaseModel


class DBPlatformDatabase(DBModelBase[PlatformDatabaseModel]):
    __tablename__ = 'platform_databases'

    id: Mapped[int] = mapped_column(primary_key=True)
    platform: Mapped[str] = mapped_column(String(20), nullable=False)
    name: Mapped[str] = mapped_column(String(20), nullable=True)
    is_default: Mapped[bool] = mapped_column(Boolean())

    db_path: Mapped[str] = mapped_column(String(120), nullable=False, unique=True)
    content: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSON()), nullable=False, default={})
    last_content_update: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=func.now())

    _pydantic_model = PlatformDatabaseModel


M_DBPlatformDatabase = TypedDict("M_DBPlatformDatabase",
                                 {
                                     "id": int,
                                     "platform": str,
                                     "connection_str": str
                                 })


def db_m2dict(item: Base) -> dict:
    return {
        column.key: getattr(item, column.key)
        for column in type(item).__table__.columns
    }


# todo turn to Pydantic model
@dataclass
class CollectionResult:
    posts: list[DBPost]
    added_posts: list[PostModel]
    users: list[DBUser]
    task: ClientTaskConfig
    duration: int
    collected_items: int
    execution_ts: SerializableDatetimeAlways


def get_orm_classes() -> dict[str, Base]:
    return {
        c.__tablename__: c for c in [DBCollectionTask, DBPost, DBComment]
    }
