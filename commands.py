from typing import Annotated

from pathlib import Path

from big5_databases.databases.db_mgmt import DatabaseManager
from big5_databases.databases.db_utils import get_collected_posts_by_period, get_posts_by_period
from big5_databases.databases.external import TimeWindow

try:
    import typer
except ModuleNotFoundError:
    print("Module typer missing [optional dependency: 'commands']")
    import sys

    sys.exit(1)

app = typer.Typer(name="Databases commands",
                  short_help="Database commands for stats and edits")


@app.command(short_help="collected_posts_per_day")
def collected_per_day(db_path: Annotated[Path, typer.Argument()],
                      period: Annotated[str, typer.Argument(help="day,month,year")] = "day"):
    db = DatabaseManager.sqlite_db_from_path(db_path)
    assert period in ["day", "month", "year"]
    print(get_collected_posts_by_period(db, TimeWindow(period)))


@app.command("posts_per_period", short_help="posts by period")
def posts_per_period(db_path: Annotated[Path, typer.Argument()],
                     period: Annotated[str, typer.Argument(help="day,month,year")] = "day"):
    db = DatabaseManager.sqlite_db_from_path(db_path)
    assert period in ["day", "month", "year"]
    print(get_posts_by_period(db, TimeWindow(period)))

@app.command("get_missing_days")
def get_missing_days(db_path: Annotated[Path, typer.Argument()]):
    # db = DatabaseManager.sqlite_db_from_path(db_path)
    raise NotImplementedError
