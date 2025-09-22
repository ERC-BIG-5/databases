import calendar
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Annotated, Optional, Any

from rich.console import Console
from rich.table import Table, Column

from big5_databases.databases.c_db_merge import check_for_conflicts
from big5_databases.databases.db_analytics import get_collected_posts_by_period, get_posts_by_period
from big5_databases.databases.db_mgmt import DatabaseManager
from big5_databases.databases.db_settings import SqliteSettings
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
           databases: Annotated[Optional[list[str]], typer.Argument()] = None):
    results: list[dict[str, Any]] = MetaDatabase().general_databases_status(databases, task_status, force_refresh)
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
    table = Table("date", "time", "posts", title=f"{db.metadata.name} posts per {period}")
    for date_posts in ppd:
        row = [str(_) for _ in date_posts]
        if period == "day":
            row.insert(1, date.fromisoformat(date_posts[0]).strftime("%A")[:3])
        elif period == "month":
            row.insert(1, calendar.month_name[int(date_posts[0].split("-")[1])][:3])
        else:
            row.insert(1, date_posts[0])
        table.add_row(*row)
    if print_:
        Console().print(table)
    return ppd


@app.command(short_help="add a db-path to some metadatabase")
def add(db_path: Annotated[str, typer.Argument()],
        platform: Annotated[str, typer.Argument()],
        name: Annotated[str, typer.Argument()],
        meta_db_path: Annotated[Optional[str], typer.Argument()] = None):
    pdb = PlatformDatabaseModel(platform=platform, name=name, db_path=Path(db_path))
    assert pdb.exists(), f"database at path: {db_path} does not exist"
    MetaDatabase(meta_db_path).add_db(pdb)


@app.command(short_help="remove a database", help="Remove a database from the main-db. Also ask user if they want to delete the file. it will be renamed to DEL_<filename> otherwise")
def remove(db_name: Annotated[str, typer.Argument(autocompletion=get_db_names)]):
    MetaDatabase().delete(db_name)


@app.command(short_help="rename a database")
def rename(db_name: Annotated[str, typer.Argument(autocompletion=get_db_names)],
           new_db_name: Annotated[str, typer.Argument()]):
    MetaDatabase().rename(db_name, new_db_name)


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


@app.command()
def base_dbs_path():
    print(SqliteSettings().default_sqlite_dbs_base_path)


@app.command()
def set_path(
        db_name: Annotated[str, typer.Argument(autocompletion=get_db_names)],
        new_path: Annotated[str, typer.Argument()]):
    MetaDatabase().move_database(db_name, new_path)


@app.command(short_help="alternative paths are used for syncing, add moving post metadata_content around")
def set_alternative_path(
        db_name: Annotated[str, typer.Argument(autocompletion=get_db_names)],
        alternative_path_name: Annotated[str, typer.Argument()],
        alternative_path: Annotated[Path, typer.Argument()]
):
    db = MetaDatabase().get(db_name)
    if not alternative_path.is_absolute():
        alternative_path = SqliteSettings().default_sqlite_dbs_base_path / alternative_path
    assert Path(alternative_path).exists(), f"alternative_path does not exist: {alternative_path}"
    MetaDatabase().set_alternative_path(db_name, alternative_path_name, Path(alternative_path))

@app.command(short_help="get alternative paths")
def get_alternative_paths(
        db_name: Annotated[str, typer.Argument(autocompletion=get_db_names)]
):
    print(MetaDatabase().get(db_name).content.alternative_paths)

@app.command()
def remove_alternative_path(
        db_name: Annotated[str, typer.Argument(autocompletion=get_db_names)],
        alternative_name: Annotated[str, typer.Argument()]
):
    def _remove_alt(session, db):
        del db.content.get("alternative_paths",{})[alternative_name]
        from sqlalchemy.orm.attributes import flag_modified
        flag_modified(db,"content")

    MetaDatabase().edit(db_name, _remove_alt)

@app.command()
def copy_posts_metadata_content(db_name: Annotated[str, typer.Argument()],
                                alternative_name: Annotated[str, typer.Argument()],
                                field: Annotated[str, typer.Argument()],
                                direction: Annotated[str, typer.Argument()] = "to_alternative",
                                overwrite: Annotated[bool, typer.Argument()] = False):
    assert direction in ["to_alternative", "to_main"]
    MetaDatabase().copy_posts_metadata_content(db_name, alternative_name, field, direction, overwrite)
