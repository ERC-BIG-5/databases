import calendar
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Annotated, Optional, Any

from rich.console import Console
from rich.table import Table, Column
from sqlalchemy.sql.functions import func

from big5_databases.databases.c_db_merge import check_for_conflicts
from big5_databases.databases.db_analytics import get_collected_posts_by_period, get_posts_by_period
from big5_databases.databases.db_models import DBPost
from big5_databases.databases.db_settings import SqliteSettings, DatabaseSettings
from big5_databases.databases.external import TimeWindow, DatabaseRunState, PlatformDBConfig, SQliteConnection
from big5_databases.databases.meta_database import MetaDatabase
from big5_databases.databases.model_conversion import PlatformDatabaseModel
from big5_databases.databases.platform_db_mgmt import PlatformDB
from big5_databases.databases.post_analysis_db import create_packaged_databases, proc_package_method
from rich import print
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
           no_refresh: bool = False,
           force_refresh: bool = False,
           databases: Annotated[Optional[list[str]], typer.Argument()] = None):
    results: list[dict[str, Any]] = MetaDatabase().general_databases_status(databases, task_status, no_refresh,
                                                                            force_refresh)
    table = Table(*[Column(c, justify="right") for c in results[0].keys()])
    for r in results:
        table.add_row(*r.values())
    Console().print(table)


@app.command(short_help="collected_posts_per_day")
def collected_per_day(db_name: Annotated[str, typer.Argument(autocompletion=get_db_names)],
                      period: Annotated[str, typer.Argument(help="day,month,year")] = "day",
                      dump_to_file: Annotated[Optional[Path], typer.Argument(help="path of file")] = None):
    assert period in ["day", "month", "year"]
    db = MetaDatabase().get_platform_db(db_name)
    col_per_day = get_collected_posts_by_period(db, TimeWindow(period))
    header = ["date", "# tasks", "found", "added"]
    theader = [Column(h, justify="right") for h in header]
    table = Table(*theader, title=db.metadata.name)

    for date, posts in col_per_day.items():
        table.add_row(str(date), *[str(_) for _ in posts.values()])
    Console().print(table)
    if dump_to_file:
        json.dump(col_per_day, dump_to_file.open("w"))


@app.command(short_help="posts by period")
def posts_per_period(db_name: Annotated[str, typer.Argument(autocompletion=get_db_names)],
                     period: Annotated[str, typer.Argument(help="day,month,year")] = "day",
                     print_: Annotated[bool, typer.Option()] = True,
                     dump_to_file: Annotated[Optional[Path], typer.Option(help="dump to file")] = None):
    db = MetaDatabase().get_platform_db(db_name)
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
    if dump_to_file:
        json.dump(ppd, dump_to_file.open("w"))


@app.command()
def base_dbs_path() -> str:
    p = SqliteSettings().default_sqlite_dbs_base_path
    print(SqliteSettings().default_sqlite_dbs_base_path)
    return str(p)


def add_db_path_help() -> str:
    return f"absolute path or path relative to: '{base_dbs_path()}'"

@app.command(short_help="add a db-path to some metadatabase")
def add(db_path: Annotated[str, typer.Argument(help=add_db_path_help())],
        platform: Annotated[str, typer.Argument()],
        name: Annotated[str, typer.Argument()],
        meta_db_path: Annotated[Optional[str], typer.Argument()] = None):
    """
    Parameters
    ----------
    db_path
    platform
    name
    meta_db_path

    Returns
    -------

    """
    pdb = PlatformDatabaseModel(platform=platform, name=name, db_path=Path(db_path))
    assert pdb.exists(), f"database at path: {db_path} does not exist"
    MetaDatabase(meta_db_path).add_db(pdb)


@app.command(short_help="remove a database",
             help="Remove a database from the main-db. Also ask user if they want to delete the file. it will be renamed to DEL_<filename> otherwise")
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
def recent_collection(
        db_name: Annotated[str, typer.Option(autocompletion=get_db_names)] = None,
        days: Annotated[int, typer.Option()] = 3,
        include_last_tasks: Annotated[int, typer.Option()] = 3):
    t = datetime.today() - timedelta(days=days)
    header = ["platform", "date", "# tasks", "found", "added"]
    theader = [Column(h, justify="right") for h in header]
    table = Table(*theader, title="recent downloads")
    meta_db = MetaDatabase()
    if db_name:
        dbs = [meta_db.get(db_name)]
    else:
        dbs = meta_db.get_dbs()
    for db in dbs:
        # print(db.name)
        col_per_day = get_collected_posts_by_period(meta_db.get_platform_db(db.name), TimeWindow.DAY, t,
                                                    include_last_tasks)
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
def set_path(
        db_name: Annotated[str, typer.Argument(autocompletion=get_db_names)],
        new_path: Annotated[Path, typer.Argument()]):
    MetaDatabase(check_databases=False).set_db_path(db_name, new_path)


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
        del db.content.get("alternative_paths", {})[alternative_name]
        from sqlalchemy.orm.attributes import flag_modified
        flag_modified(db, "content")

    MetaDatabase().edit(db_name, _remove_alt)


@app.command()
def copy_posts_metadata_content(db_name: Annotated[str, typer.Argument(autocompletion=get_db_names)],
                                alternative_name: Annotated[str, typer.Argument()],
                                field: Annotated[str, typer.Argument()],
                                direction: Annotated[str, typer.Argument()] = "to_alternative",
                                overwrite: Annotated[bool, typer.Argument()] = False):
    assert direction in ["to_alternative", "to_main"]
    MetaDatabase().copy_posts_metadata_content(db_name, alternative_name, field, direction, overwrite)


@app.command()
def create_proc_db(db_name: Annotated[str, typer.Argument(autocompletion=get_db_names)],
                   data_type: Annotated[str, typer.Argument(autocompletion=lambda: ["text", "media"])],
                   proc_db_path: Annotated[Optional[Path], typer.Argument()] = None):
    create_packaged_databases([db_name], proc_db_path,
                              proc_package_method(data_type), delete_destination=False, exists_ok=True)


@app.command()
def get_full_path(db_name: str) -> Path:
    fp = MetaDatabase().get(db_name).full_path
    print(fp)
    return fp


@app.command(short_help="Manually add a running state")
def add_run_state(
        db_name: Annotated[str, typer.Argument(autocompletion=get_db_names)],
        pipeline_method: Annotated[str, typer.Argument()],
        alt_db_name: Annotated[Optional[str], typer.Argument()] = None,
        location: Annotated[Optional[str], typer.Argument()] = None
):
    if not location:
        location = DatabaseSettings().location

    MetaDatabase().add_run_state(db_name,
                                 DatabaseRunState(
                                     pipeline_method=pipeline_method,
                                     location=location,
                                     alt_db=alt_db_name,
                                 ))


@app.command(short_help="Sample a small portion")
def sample(
        db_name: Annotated[str, typer.Argument(autocompletion=get_db_names)],
        destination: Annotated[Path, typer.Argument()],
        add_to_meta_db: Annotated[bool, typer.Option()] = False,
        overwrite: Annotated[bool, typer.Option()] = False,
        sample_size: Annotated[int, typer.Argument(max=5000)] = 100
):
    meta_db = MetaDatabase()
    if not meta_db.exists(db_name):
        print(f"error, db : {db_name} does not exist")
        return

    # this will also set the absolute correct path
    db_connection = SQliteConnection(db_path=destination)
    final_destination = Path(db_connection.db_path)
    if final_destination != destination:
        print(f"! Destination set to: {final_destination}")

    if final_destination.exists():
        if not overwrite:
            print(f"❌ error, db at '{final_destination}' exists already (set ---overwrite)")
            return
        else:
            final_destination.unlink()

    db = meta_db.get_platform_db(db_name)
    sample_db = PlatformDB(PlatformDBConfig(platform=db.platform, create=True, db_connection=db_connection,
                                            require_existing_parent_dir=True))
    with db.get_session() as session:
        posts: list[DBPost] = session.query(DBPost).order_by(func.random()).limit(sample_size).all()
        mod_posts = [p.model() for p in posts]
        for p in mod_posts:
            p.collection_task_id = None
        submitted = sample_db.safe_submit_posts(mod_posts)
        print(f"✓ done! DB at {str(final_destination)}, {len(submitted)} posts")

    if add_to_meta_db:
        name_ts_postfix = datetime.now().strftime("%Y%m%d_%H%M")
        meta_db.add_db(sample_db.get_model(db_name=f"{db_name}_{name_ts_postfix}.sqlite"))
