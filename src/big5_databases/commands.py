from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Annotated, Optional, Any

from rich.console import Console
from rich.table import Table, Column

from big5_databases.databases.c_db_merge import check_for_conflicts
from big5_databases.databases.db_mgmt import DatabaseManager
from big5_databases.databases.db_utils import get_collected_posts_by_period, get_posts_by_period
from big5_databases.databases.external import TimeWindow
from big5_databases.databases.meta_database import MetaDatabase
from big5_databases.databases.model_conversion import PlatformDatabaseModel

try:
    import typer
except ModuleNotFoundError:
    print("Module typer missing [optional dependency: 'commands']")
    import sys

    sys.exit(1)

app = typer.Typer(name="Databases commands",
                  short_help="Database commands for stats and edits")


def get_db_names() -> list[str]:
    return [db.name for db in MetaDatabase().get_dbs()]


@app.command(short_help="Get the number of posts, and tasks statuses of all specified databases (RUN_CONFIG)")
def status(task_status: bool = True,
           force_refresh: bool = False,
           database: Optional[Path] = None):
    results: list[dict[str, Any]] = MetaDatabase().general_databases_status(database, task_status, force_refresh)
    table = Table(*[Column(c, justify="right") for c in results[0].keys()])
    for r in results:
        table.add_row(*r.values())
    Console().print(table)


@app.command(short_help="collected_posts_per_day")
def collected_per_day(db_name: Annotated[str, typer.Argument(autocompletion=get_db_names)],
                      period: Annotated[str, typer.Argument(help="day,month,year")] = "day"):
    assert period in ["day", "month", "year"]
    db = db = MetaDatabase().get_db_mgmt(db_name)
    col_per_day = get_collected_posts_by_period(db, TimeWindow(period))
    header = ["date", "# tasks", "found", "added"]
    header = [Column(h, justify="right") for h in header]
    table = Table(*header, title=db.metadata.name)

    for date, posts in col_per_day.items():
        table.add_row(str(date), *[str(_) for _ in posts.values()])
    Console().print(table)


@app.command(short_help="posts by period")
def posts_per_period(db_name: Annotated[str, typer.Argument(autocompletion=get_db_names)],
                     period: Annotated[str, typer.Argument(help="day,month,year")] = "day",
                     print_: Annotated[bool, typer.Argument()] = True):
    db = MetaDatabase().get_db_mgmt(db_name)
    assert period in ["day", "month", "year"]
    ppd = get_posts_by_period(db, TimeWindow(period))
    table = Table("date", "posts", title=f"{db.metadata.name} posts per {period}")
    for date_posts in ppd:
        row = [str(_) for _ in date_posts]
        if period == "day":
            row.insert(1, date.fromisoformat(date_posts[0]).strftime("%A")[:3])
        table.add_row(*row)
    if print_:
        Console().print(table)
    return ppd


@app.command(short_help="add a db-path to some metadatabase")
def add(db_path: Annotated[str, typer.Argument()],
        platform: str,
        name: str,
        meta_db_path: Annotated[Optional[str], typer.Argument()] = None):
    assert Path(db_path).exists(), f"database at path: {db_path} does not exist"
    MetaDatabase(meta_db_path).add_db(PlatformDatabaseModel(platform=platform, name=name, db_path=Path(db_path)))


@app.command(short_help="remove a database")
def remove(db_name: Annotated[str, typer.Argument(autocompletion=get_db_names)]):
    MetaDatabase().delete(db_name)


@app.command(short_help="compare two databases (prep for merge")
def compare_dbs(db_path1: Annotated[str, typer.Argument()],
                db_path2: Annotated[str, typer.Argument()]):
    check_for_conflicts(db_path1, db_path2)


@app.command("recent-collection",
             short_help="get recent collection stats")
def recent_collection(days: Annotated[int, typer.Argument()] = 3):
    t = datetime.today() - timedelta(days=days)
    header = ["platform", "date", "# tasks", "found", "added"]
    header = [Column(h, justify="right") for h in header]
    table = Table(*header, title="recent downloads")
    for db in MetaDatabase().get_dbs():
        col_per_day = get_collected_posts_by_period(db.get_mgmt(), TimeWindow.DAY, t)
        for idx, (date, posts) in enumerate(col_per_day.items()):
            table.add_row(db.name, str(date), *[str(_) for _ in posts.values()],
                          end_section=idx == len(col_per_day) - 1)
    Console().print(table)


@app.command("get_missing_days", epilog="cool")
def get_missing_days(db_path1: Annotated[str, typer.Argument()],
                     db_path2: Annotated[str, typer.Argument()]):
    # db = DatabaseManager.sqlite_db_from_path(db_path)
    raise NotImplementedError
