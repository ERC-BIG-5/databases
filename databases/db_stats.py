import json
import os
import shutil
from collections import Counter
from dataclasses import field
from datetime import date
from datetime import datetime
from pathlib import Path
from typing import Optional, Generator, Annotated

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from pydantic import BaseModel
from pydantic import field_validator
from pydantic.functional_serializers import PlainSerializer
from sqlalchemy import select

from databases import db_utils
from databases.db_mgmt import DatabaseManager
from databases.db_models import DBPost
from databases.external import DBConfig, SQliteConnection
from databases.model_conversion import PostModel
from tools.env_root import root

RAISE_DB_ERROR = True
BASE_DATA_PATH = root() / "data"
stats_copy_path = BASE_DATA_PATH / "stats_copy.sqlite"

from sqlalchemy import func

SerializableDate = Annotated[
    date, PlainSerializer(lambda d: f'{d:%Y-%m-%d}', return_type=str, when_used="always")
]

SerializableCounter = Annotated[
    Counter[str], PlainSerializer(lambda c: dict(c), return_type=dict)
]

SerializablePath = Annotated[
    Path, PlainSerializer(lambda p: p.relative_to(BASE_DATA_PATH).as_posix(), return_type=str)
]


class PlatformStats(BaseModel):
    name: str
    post_count: int = 0
    min_date: SerializableDate = field(default_factory=datetime.max.date)
    max_date: SerializableDate = field(default_factory=datetime.min.date)
    year_month_count: Counter[str] = field(default_factory=Counter)
    last_collected: SerializableDate = field(default_factory=datetime.min.date)
    date_count: dict[str, int] = field(default_factory=dict)

    def add_post(self, post: DBPost):
        self.post_count += 1
        created = post.date_created.date()
        self.min_date = min(self.min_date, created)
        self.max_date = max(self.max_date, created)
        self.year_month_count[f"{created:%Y_%m}"] += 1
        self.last_collected = max(self.last_collected, post.date_collected.date())

    def add_day_counts(self, date: str, count: int):
        date_ = datetime.strptime(date, "%Y-%m-%d").date()
        self.min_date = min(self.min_date, date_)
        self.max_date = max(self.max_date, date_)
        self.date_count[date] = count


class DBStats(BaseModel):
    db_path: SerializablePath
    platforms: dict[str, PlatformStats] = field(default_factory=dict)
    error: Optional[str] = None
    file_size: int

    @field_validator("db_path")
    def validate_db_path(cls, v):
        v = root() / "data" / v
        return v

    def add_post(self, post: DBPost | PostModel):
        self.platforms.setdefault(post.platform, PlatformStats(name=post.platform)).add_post(post)

    def add_day_counts(self, post: DBPost, date: date, count: int):
        self.platforms.setdefault(post.platform, PlatformStats(name=post.platform)).add_day_counts(date, count)

    def plot_daily_items(self, platform: str, bars: bool = False):

        plt.figure(figsize=(12, 6))

        daily_counts = pd.Series(self.platforms[platform].date_count)
        # Convert index to datetime if not already
        if not isinstance(daily_counts.index, pd.DatetimeIndex):
            daily_counts.index = pd.to_datetime(daily_counts.index)

        if bars:
            plt.bar(daily_counts.index, daily_counts.values,
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

        plt.title('Daily Post Count (Red Bars = No Posts)')
        plt.xlabel('Date')
        plt.ylabel('Number of Posts')

        # Improved x-axis labels
        plt.gca().xaxis.set_major_locator(mdates.MonthLocator())
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
        plt.xticks(rotation=45)

        plt.grid(True, alpha=0.3)
        # plt.legend()
        plt.tight_layout()
        return plt


def make_stats_copy(db_path: Path):
    shutil.copy(db_path, stats_copy_path)


def delete_stats_copy():
    if os.path.exists(stats_copy_path):
        os.remove(stats_copy_path)


def get_posts(db: DatabaseManager) -> Generator[PostModel, None, None]:
    with db.get_session() as session:
        query = select(DBPost)

        # Execute the query and return the results
        result = session.execute(query).scalars()
        for post in result:
            yield post.model()


def get_posts_by_day(db: DatabaseManager) -> Generator[tuple[DBPost, date, int], None, None]:
    with db.get_session() as session:
        query = select(
            DBPost,
            func.date(DBPost.date_created).label('day'),
            func.count().label('count')
        ).group_by(
            func.date(DBPost.date_created)
        )

        # Execute the query and return the results
        result = session.execute(query).all()
        for post, date_, count in result:
            yield post, date_, count


def generate_db_stats(db_path: Path,
                      daily_details: bool = False) -> DBStats:
    make_stats_copy(db_path)

    db_func = get_posts
    if daily_details:
        db_func = get_posts_by_day

    try:
        db = DatabaseManager.sqlite_db_from_path(stats_copy_path)
        _stats = DBStats(db_path=db_path, file_size=db_utils.file_size(db))
        for res in db_func(db):
            if daily_details:
                _stats.add_day_counts(*res)
            else:
                assert isinstance(res, PostModel)
                _stats.add_post(res)
    except Exception as e:
        if RAISE_DB_ERROR:
            raise e
        print(e)
        _stats.error = str(e)
    finally:
        delete_stats_copy()
    # print(stats)
    return _stats


def count_posts(*,
                db_path: Optional[Path] = None,
                db_manager: Optional[DatabaseManager] = None) -> int:
    if not db_manager:
        if not db_path:
            raise TypeError('db_path or db_manager must be provided')
        db_manager = DatabaseManager(DBConfig(db_connection=SQliteConnection(db_path=db_path)))

    with db_manager.get_session() as session:
        count = session.execute(select(func.count()).select_from(DBPost)).scalar()  # 4.3338918359986565
        return count


if __name__ == "__main__":
    stats = generate_db_stats(BASE_DATA_PATH / "youtube2024.sqlite", True)
    print(json.dumps(stats.model_dump(), indent=2))
    plt = stats.plot_daily_items("youtube")
    plt.show()
