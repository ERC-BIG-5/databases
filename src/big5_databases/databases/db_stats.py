import json
from collections import Counter

from databases import db_utils
from databases.db_mgmt import DatabaseManager
from databases.db_utils import get_posts_by_period
from databases.external import SQliteConnection, DBStats, TimeWindow, TimeColumn
from tools.env_root import root

RAISE_DB_ERROR = True


def generate_db_stats(
        db: DatabaseManager,
        time_column=TimeColumn.CREATED
) -> DBStats:
    """
    Generate statistics for a database using the specified period.

    Args:
        db: Database manager instance
        time_column: created or collected
    Returns:
        DBStats object containing the statistics
    """
    try:
        # Ensure we're working with a SQLite database
        assert isinstance(db.config.db_connection, SQliteConnection)

        # Create the stats object
        stats = DBStats(
            db_path=db.config.db_connection.db_path,
            period=TimeWindow.DAY,
            file_size=db_utils.file_size(db)
        )

        # Populate with data from the database
        created, collected = get_posts_by_period(db, TimeWindow.DAY)
        for period_str, count in created:
            stats.created_counts.set(period_str, count)
        for period_str, count in collected:
            stats.collected_counts.set(period_str, count)
        return stats

    except Exception as e:
        # Create an error stats object
        if RAISE_DB_ERROR:
            raise e
        error_stats = DBStats(
            db_path=db.config.db_connection.db_path,
            period=TimeWindow.DAY,
            error=str(e),
            file_size=db_utils.file_size(db) if hasattr(db, 'config') else 0,
            time_column=time_column
        )
        return error_stats


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
    db = DatabaseManager.sqlite_db_from_path(root() / "data/tiktok.sqlite", False)
    stats = generate_db_stats(db)
    print(json.dumps(stats.model_dump(), indent=2))

    # stats = generate_db_stats(db, "month")
    # print(json.dumps(stats.model_dump(), indent=2))

    # stats = generate_db_stats(db, "year")
    # print(json.dumps(stats.model_dump(), indent=2))
    plt = stats.plot_daily_items(bars=True,period=TimeWindow.DAY)
    plt.show()
