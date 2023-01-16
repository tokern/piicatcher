import json
import logging
import logging.config
from contextlib import closing
from pathlib import Path
from typing import List, Optional

import dbcat.settings
from sqlalchemy.orm.exc import NoResultFound
import typer
from dbcat.api import init_db, open_catalog
from dbcat.cli import app as catalog_app
from dbcat.cli import (
    exclude_schema_help_text,
    exclude_table_help_text,
    schema_help_text,
    table_help_text,
)
from dbcat.generators import NoMatchesError
from pythonjsonlogger import jsonlogger
from tabulate import tabulate

from piicatcher import __version__
from piicatcher.api import (
    OutputFormat,
    ScanTypeEnum,
    list_detector_entry_points,
    list_detectors,
    scan_database,
)
from piicatcher.generators import SMALL_TABLE_MAX
from piicatcher.scanner import data_logger, scan_logger

app = typer.Typer()

LOGGER = logging.getLogger(__name__)


class TyperLoggerHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        fg = None
        bg = None
        if record.levelno == logging.DEBUG:
            fg = typer.colors.YELLOW
        elif record.levelno == logging.INFO:
            fg = typer.colors.BRIGHT_BLUE
        elif record.levelno == logging.WARNING:
            fg = typer.colors.BRIGHT_MAGENTA
        elif record.levelno == logging.CRITICAL:
            fg = typer.colors.BRIGHT_RED
        elif record.levelno == logging.ERROR:
            fg = typer.colors.BRIGHT_WHITE
            bg = typer.colors.RED
        typer.secho(self.format(record), bg=bg, fg=fg)


def log_config(log_level: str):
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "simple": {
                "format": "{asctime}:{levelname}:{name} {message}",
                "style": "{",
            },
        },
        "handlers": {
            "console": {"class": "logging.StreamHandler", "formatter": "simple"},
            "typer": {
                "class": "dbcat.__main__.TyperLoggerHandler",
                "formatter": "simple",
            },
        },
        "root": {"handlers": ["typer"], "level": log_level},
        "dbcat": {"handlers": ["typer"], "level": log_level},
        "piicatcher": {"handlers": ["typer"], "level": log_level},
        "sqlachemy": {"handlers": ["typer"], "level": "DEBUG"},
        "alembic": {"handlers": ["typer"], "level": "DEBUG"},
        "databuilder": {"handlers": ["typer"], "level": "INFO"},
    }


def version_callback(value: bool):
    if value:
        print("{}".format(__version__))
        typer.Exit()


def stats_callback(value: bool):
    if value:
        print("{}".format(__version__))
        typer.Exit()


# pylint: disable=too-many-arguments
@app.callback(invoke_without_command=True)
def cli(
        log_level: str = typer.Option("WARNING", help="Logging Level"),
        log_data: bool = typer.Option(False, help="Choose output format type"),
        log_scan: bool = typer.Option(False, help="Log data that was scanned"),
        config_path: Path = typer.Option(
            typer.get_app_dir("tokern"), help="Path to config directory"
        ),
        output_format: OutputFormat = typer.Option(
            OutputFormat.tabular, case_sensitive=False
        ),
        catalog_path: Path = typer.Option(
            None, help="Path to store catalog state. Use if NOT using a database"
        ),
        catalog_host: str = typer.Option(
            None, help="hostname of Postgres database. Use if catalog is a database."
        ),
        catalog_port: int = typer.Option(
            None, help="port of Postgres database. Use if catalog is a database."
        ),
        catalog_user: str = typer.Option(
            None, help="user of Postgres database. Use if catalog is a database."
        ),
        catalog_password: str = typer.Option(
            None, help="password of Postgres database. Use if catalog is a database."
        ),
        catalog_database: str = typer.Option(
            None, help="database of Postgres database. Use if catalog is a database."
        ),
        catalog_secret: str = typer.Option(
            dbcat.settings.DEFAULT_CATALOG_SECRET,
            help="Secret to encrypt sensitive data like passwords in the catalog.",
        ),
        version: Optional[bool] = typer.Option(
            None, "--version", callback=version_callback, is_eager=True
        ),
        stats_status: Optional[bool] = typer.Option(
            None, "--disable-stats", callback=stats_callback, is_eager=True
        ),
):
    logging.config.dictConfig(log_config(log_level=log_level.upper()))

    if log_scan:
        handler = logging.StreamHandler()
        handler.setFormatter(jsonlogger.JsonFormatter())
        handler.setLevel(logging.INFO)
        scan_logger.addHandler(handler)
        LOGGER.debug("SCAN LOG setup")

    if log_data:
        handler = logging.StreamHandler()
        handler.setFormatter(jsonlogger.JsonFormatter())
        handler.setLevel(logging.INFO)
        data_logger.addHandler(handler)
        LOGGER.debug("DATA LOG setup")

    app_dir_path = Path(config_path)
    app_dir_path.mkdir(parents=True, exist_ok=True)

    dbcat.settings.CATALOG_PATH = catalog_path
    dbcat.settings.CATALOG_USER = catalog_user
    dbcat.settings.CATALOG_PASSWORD = catalog_password
    dbcat.settings.CATALOG_HOST = catalog_host
    dbcat.settings.CATALOG_PORT = catalog_port
    dbcat.settings.CATALOG_DB = catalog_database
    dbcat.settings.CATALOG_SECRET = catalog_secret
    dbcat.settings.APP_DIR = app_dir_path
    dbcat.settings.OUTPUT_FORMAT = output_format


@app.command()
def detect(
        source_name: str = typer.Option(..., help="Name of database to scan."),
        scan_type: ScanTypeEnum = typer.Option(
            ScanTypeEnum.metadata,
            help="Choose deep(scan data) or shallow(scan column names only)",
        ),
        incremental: bool = typer.Option(
            True, help="Scan columns updated or created since last run",
        ),
        list_all: bool = typer.Option(
            False,
            help="List all columns. By default only columns with PII information is listed",
        ),
        include_schema: Optional[List[str]] = typer.Option(None, help=schema_help_text),
        exclude_schema: Optional[List[str]] = typer.Option(
            None, help=exclude_schema_help_text
        ),
        include_table: Optional[List[str]] = typer.Option(None, help=table_help_text),
        exclude_table: Optional[List[str]] = typer.Option(
            None, help=exclude_table_help_text
        ),
        sample_size: int = typer.Option(
            SMALL_TABLE_MAX, help="Sample size for large tables when running deep scan."
        ),
):
    catalog = open_catalog(
        app_dir=dbcat.settings.APP_DIR,
        secret=dbcat.settings.CATALOG_SECRET,
        path=dbcat.settings.CATALOG_PATH,
        host=dbcat.settings.CATALOG_HOST,
        port=dbcat.settings.CATALOG_PORT,
        user=dbcat.settings.CATALOG_USER,
        password=dbcat.settings.CATALOG_PASSWORD,
        database=dbcat.settings.CATALOG_DB,
    )

    with closing(catalog) as catalog:
        init_db(catalog)
        with catalog.managed_session:
            try:
                source = catalog.get_source(source_name)
                op = scan_database(
                    catalog=catalog,
                    source=source,
                    scan_type=scan_type,
                    incremental=incremental,
                    output_format=dbcat.settings.OUTPUT_FORMAT,
                    list_all=list_all,
                    include_schema_regex=include_schema,
                    exclude_schema_regex=exclude_schema,
                    include_table_regex=include_table,
                    exclude_table_regex=exclude_table,
                    sample_size=sample_size,
                )
                typer.echo(message=str_output(op, dbcat.settings.OUTPUT_FORMAT))
            except NoMatchesError:
                typer.echo(message=NoMatchesError.message)
                typer.Exit(1)
            except NoResultFound:
                typer.echo("no catalog with given name exist. Please use catalog command to add catalog.")
                typer.Exit(1)


detector_app = typer.Typer()


@detector_app.command(name="list")
def cli_list_detectors():
    typer.echo(
        message=tabulate(
            tabular_data=[(d,) for d in list_detectors()], headers=("detectors",)
        )
    )


@detector_app.command(name="entry-points")
def cli_list_entry_points():
    typer.echo(
        message=tabulate(
            tabular_data=[(e,) for e in list_detector_entry_points()],
            headers=("entry points",),
        )
    )


app.add_typer(detector_app, name="detectors")
app.add_typer(catalog_app, name="catalog")


def str_output(op, output_format: OutputFormat):
    if output_format == OutputFormat.tabular:
        return tabulate(
            tabular_data=op,
            headers=("schema", "table", "column", "PII Type", "Scanner"),
        )
    else:
        return json.dumps(op, sort_keys=True, indent=2)
