from dataclasses import dataclass
from datetime import datetime
from typing import TypedDict, TypeVar, Generic

from sqlalchemy import Column, String, Integer, Boolean, DateTime, ForeignKey, JSON, Enum, func, UniqueConstraint
from sqlalchemy import Enum as SQLAlchemyEnum
from sqlalchemy.orm import relationship, Mapped, mapped_column, declarative_base
from pydantic import BaseModel
from databases.external import CollectionStatus, ClientTaskConfig
from databases.external import PostType
from databases.model_conversion import PlatformDatabaseModel, CollectionTaskModel, PostModel

Base = declarative_base()

T = TypeVar('T', bound=BaseModel)


class EmptyModel(BaseModel):
    pass


class DBModelBase(Generic[T], Base):
    """
    Generic base class for all database models that can be converted to Pydantic models
    """
    __abstract__ = True
    _pydantic_model = EmptyModel

    def model(self) -> T:
        return self._pydantic_model.model_validate(self, from_attributes=True)


class DBUser(Base):
    __tablename__ = 'user'

    id: Mapped[int] = mapped_column(primary_key=True)
    platform: Mapped[str] = mapped_column(String(20), nullable=False)
    platform_username: Mapped[str] = mapped_column(String(20), nullable=True)

    # posts: Mapped[list["DBPost"]] = relationship(back_populates="user")


# class DBPostContentSchema(Base):
#     __tablename__ = 'post_content_schema'
#
#     id: Mapped[int] = mapped_column(primary_key=True)
#     platform: Mapped[str] = mapped_column(String(20), nullable=False)
#     schema: Mapped[dict] = mapped_column(JSON, nullable=False)
#     posts: Mapped[list["DBPost"]] = relationship(back_populates="content_schema")


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
    found_items: Mapped[int] = mapped_column(Integer, nullable=True)
    added_items: Mapped[int] = mapped_column(Integer, nullable=True)

    collection_duration: Mapped[int] = mapped_column(Integer, nullable=True)  # in millis
    status: Mapped[CollectionStatus] = mapped_column(SQLAlchemyEnum(CollectionStatus), nullable=False,
                                                     default=CollectionStatus.INIT)
    transient: Mapped[bool] = mapped_column(Boolean, nullable=True, default=False)
    time_added: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    database: Mapped[str] = mapped_column(String(20), nullable=True)

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
    #
    platform: Mapped[str] = mapped_column(String(20), nullable=False)
    platform_id: Mapped[str] = mapped_column(String(50), nullable=False, unique=True)
    date_created: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    content: Mapped[dict] = Column(JSON)
    post_url: Mapped[str] = mapped_column(String(60), nullable=True)
    post_type: Mapped[PostType] = mapped_column(Enum(PostType), nullable=False, default=PostType.REGULAR)
    #
    metadata_content: Mapped[dict] = Column(JSON, default=dict)
    date_collected: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())

    # todo: temp nullable
    collection_task: Mapped["DBCollectionTask"] = relationship(backref="posts")
    collection_task_id: Mapped[int] = mapped_column(ForeignKey("collection_task.id"), nullable=True)

    # collection_step: Mapped[int] = mapped_column(Integer, nullable=True)

    # user_id: Mapped[int] = mapped_column(ForeignKey("user.id"))
    # user: Mapped[DBUser] = relationship(back_populates="posts")

    # content_schema_id: Mapped[int] = mapped_column(ForeignKey("post_content_schema.id"), nullable=True)
    # content_schema: Mapped[DBPostContentSchema] = relationship(back_populates="posts")

    comments: Mapped[list[DBComment]] = relationship(back_populates="post")

    _pydantic_model = PostModel


class DBPlatformDatabase(DBModelBase[PlatformDatabaseModel]):
    __tablename__ = 'platform_databases'

    id: Mapped[int] = mapped_column(primary_key=True)
    platform: Mapped[str] = mapped_column(String(20), nullable=False, unique=True)
    name: Mapped[str] = mapped_column(String(20), nullable=True)
    connection_str: Mapped[str] = mapped_column(String(), nullable=False)
    is_default: Mapped[bool] = mapped_column(Boolean())

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


@dataclass
class CollectionResult:
    posts: list[DBPost]
    users: list[DBUser]
    task: ClientTaskConfig
    duration: int
    collected_items: int  # millis

def get_orm_classes() -> dict[str,Base]:
    return {
        c.__tablename__: c for c in [DBCollectionTask, DBPost, DBComment]
    }