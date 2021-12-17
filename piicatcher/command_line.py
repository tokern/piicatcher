import logging
import logging.config
from pathlib import Path
from typing import Optional

import typer
from pythonjsonlogger import jsonlogger

from piicatcher import __version__
from piicatcher.api import OutputFormat
from piicatcher.app_state import app_state
from piicatcher.cli import app as scan_app
from piicatcher.cli import detector_app
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
    version: Optional[bool] = typer.Option(
        None, "--version", callback=version_callback, is_eager=True
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

    app_state["catalog_connection"] = {
        "path": catalog_path,
        "user": catalog_user,
        "password": catalog_password,
        "host": catalog_host,
        "port": catalog_port,
        "database": catalog_database,
        "app_dir": app_dir_path,
    }
    app_state["output_format"] = output_format


app.add_typer(scan_app, name="scan")
app.add_typer(detector_app, name="detectors")
