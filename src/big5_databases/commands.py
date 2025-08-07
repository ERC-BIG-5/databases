from datetime import date
from pathlib import Path
from typing import Annotated

from rich.console import Console
from rich.table import Table

from big5_databases.databases.db_mgmt import DatabaseManager
from big5_databases.databases.db_utils import get_collected_posts_by_period, get_posts_by_period
from big5_databases.databases.external import TimeWindow
from big5_databases.databases.meta_database import MetaDatabase

try:
    import typer
except ModuleNotFoundError:
    print("Module typer missing [optional dependency: 'commands']")
    import sys

    sys.exit(1)

app = typer.Typer(name="Databases commands",
                  short_help="Database commands for stats and edits")


def get_db(db_path_or_name: Path | str) -> DatabaseManager:
    if isinstance(db_path_or_name, Path):
        return DatabaseManager.sqlite_db_from_path(db_path_or_name)
    else: # if SETTINGS.main_db_path:
        return MetaDatabase().get_db(db_path_or_name)


@app.command(short_help="collected_posts_per_day")
def collected_per_day(db_path: Annotated[str, typer.Argument()],
                      period: Annotated[str, typer.Argument(help="day,month,year")] = "day"):
    db = get_db(db_path)
    assert period in ["day", "month", "year"]
    col_per_day = get_collected_posts_by_period(db, TimeWindow(period))
    table = Table("date","found" ,"added", title=db.metadata.name)
    for date, posts in col_per_day.items():
        table.add_row(str(date), *[str(_) for _ in posts.values()])
    Console().print(table)


@app.command(short_help="posts by period")
def posts_per_period(db_path: Annotated[str, typer.Argument()],
                     period: Annotated[str, typer.Argument(help="day,month,year")] = "day"):
    db = get_db(db_path)
    assert period in ["day", "month", "year"]
    ppd = get_posts_by_period(db, TimeWindow(period))
    table = Table("date","posts", title=f"{db.metadata.name} posts per {period}")
    for date_posts in ppd:
        row = [str(_) for _ in date_posts]
        if period == "day":
            row.insert(1, date.fromisoformat(date_posts[0]).strftime("%A")[:3])
        table.add_row(*row)
    Console().print(table)

@app.command("get_missing_days")
def get_missing_days(db_path: Annotated[Path, typer.Argument()]):
    # db = DatabaseManager.sqlite_db_from_path(db_path)
    raise NotImplementedError
