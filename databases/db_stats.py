import json
from collections import Counter
from enum import Enum
from pathlib import Path
from typing import Optional, Annotated

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from pydantic import BaseModel
from pydantic import field_validator
from pydantic.functional_serializers import PlainSerializer
from sqlalchemy import select, func

from databases import db_utils
from databases.db_mgmt import DatabaseManager
from databases.db_models import DBPost
from databases.db_utils import base_data_path, get_posts_by_period, TimeWindow, TimeColumn
from databases.external import DBConfig, SQliteConnection
from tools.env_root import root

RAISE_DB_ERROR = True


SerializablePath = Annotated[
    Path, PlainSerializer(lambda p: p.relative_to(base_data_path()).as_posix(), return_type=str)
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


class DBStats(BaseModel):
    """Database statistics model with file information and error handling."""
    db_path: SerializablePath
    stats: RawStats = RawStats()
    period: TimeWindow = "day"
    error: Optional[str] = None
    file_size: int = 0

    @field_validator("db_path")
    def validate_db_path(cls, v):
        """Ensure db_path is absolute."""
        if not v.is_absolute():
            v = root() / "data" / v
        return v

    def add_period_count(self, period_str: str, count: int) -> None:
        """Add a count for a specific period."""
        self.stats.add(period_str, count)

    def set_period_count(self, period_str: str, count: int) -> None:
        """Set the count for a specific period."""
        self.stats.set(period_str, count)

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


def generate_db_stats(
        db: DatabaseManager,
        period: TimeWindow = "day",
        time_column = TimeColumn.CREATED
) -> DBStats:
    """
    Generate statistics for a database using the specified period.

    Args:
        db: Database manager instance
        period: Time period for grouping - "day", "month", or "year"

    Returns:
        DBStats object containing the statistics
    """
    try:
        # Ensure we're working with a SQLite database
        assert isinstance(db.config.db_connection, SQliteConnection)

        # Create the stats object
        stats = DBStats(
            db_path=db.config.db_connection.db_path,
            period=period,
            file_size=db_utils.file_size(db)
        )

        # Populate with data from the database
        for period_str, count in get_posts_by_period(db, period, time_column):
            stats.set_period_count(period_str, count)

        return stats

    except Exception as e:
        # Create an error stats object
        error_stats = DBStats(
            db_path=db.config.db_connection.db_path,
            period=period,
            error=str(e),
            file_size=db_utils.file_size(db) if hasattr(db, 'config') else 0
        )
        return error_stats


def count_posts(*,
                db_path: Optional[Path] = None,
                db_manager: Optional[DatabaseManager] = None) -> int:

    if not db_manager:
        if not db_path:
            raise TypeError('db_path or db_manager must be provided')
        db_manager = DatabaseManager(DBConfig(db_connection=SQliteConnection(db_path=db_path)))

    with db_manager.get_session() as session:
        count = session.execute(select(func.count()).select_from(DBPost)).scalar()
        return count


def validate_period_stats(day_stats: DBStats, month_stats: DBStats, year_stats: DBStats) -> None:
    """
    Validate that counts are consistent across different time periods.
    Prints validation results to console.

    Args:
        day_stats: Statistics by day
        month_stats: Statistics by month
        year_stats: Statistics by year
    """
    # Check if total counts match
    print("Validation - all totals should match:")
    print(f"Day total: {day_stats.stats.total_count}")
    print(f"Month total: {month_stats.stats.total_count}")
    print(f"Year total: {year_stats.stats.total_count}")

    # Check if counts match when aggregated
    month_from_days = Counter()
    for day_key, count in day_stats.stats.counter.items():
        # Extract YYYY-MM from YYYY-MM-DD
        month_key = day_key[:7]
        month_from_days[month_key] += count

    year_from_days = Counter()
    for day_key, count in day_stats.stats.counter.items():
        # Extract YYYY from YYYY-MM-DD
        year_key = day_key[:4]
        year_from_days[year_key] += count

    # Check for discrepancies
    print("\nChecking month totals from days vs direct month query:")
    mismatches = [(m, month_from_days[m], month_stats.stats.counter[m])
                  for m in month_from_days if month_from_days[m] != month_stats.stats.counter[m]]

    if mismatches:
        print("Mismatches found:")
        for month, days_count, month_count in mismatches:
            print(f"  {month}: {days_count} (from days) vs {month_count} (from month query)")
    else:
        print("All month totals match between aggregated days and direct month query.")


if __name__ == "__main__":
    root("/home/rsoleyma/projects/platforms-clients")
    db = DatabaseManager.sqlite_db_from_path(base_data_path() / "youtube2024.sqlite", False)
    stats = generate_db_stats(db, "day")
    print(json.dumps(stats.model_dump(), indent=2))

    stats = generate_db_stats(db, "month")
    print(json.dumps(stats.model_dump(), indent=2))

    stats = generate_db_stats(db, "year")
    print(json.dumps(stats.model_dump(), indent=2))
    # plt = stats.plot_daily_items("youtube")
    # plt.show()
