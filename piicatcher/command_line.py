import logging

import click
import click_config_file
from pythonjsonlogger import jsonlogger

from piicatcher import __version__
from piicatcher.explorer.aws import cli as aws_cli
from piicatcher.explorer.databases import cli as db_cli
from piicatcher.explorer.files import cli as files_cli
from piicatcher.explorer.metadata import data_logger, scan_logger
from piicatcher.explorer.snowflake import cli as snowflake_cli
from piicatcher.explorer.sqlite import cli as sqlite_cli


@click.group()
@click.pass_context
@click.version_option(__version__)
@click_config_file.configuration_option()
@click.option("-l", "--log-level", help="Logging Level", default="WARNING")
@click.option(
    "--catalog-format",
    type=click.Choice(["ascii_table", "json", "db", "glue"]),
    default="ascii_table",
    help="Choose catalog format type",
)
@click.option(
    "--log-data", default=False, is_flag=True, help="Log data that was scanned"
)
@click.option(
    "--log-scan",
    default=False,
    is_flag=True,
    help="Log scan events with column names and PII types",
)
@click.option(
    "--catalog-file",
    default=None,
    type=click.File("w"),
    help="File path of the catalog if format is json. If not specified, "
    "then report is printed to sys.stdout",
)
@click.option("--catalog-host", help="Hostname of the database. Use if catalog is a db")
@click.option("--catalog-port", help="Port of database. Use if catalog is a db")
@click.option(
    "--catalog-user", help="Username to connect database.  Use if catalog is a db"
)
@click.option("--catalog-password", help="Password of the user. Use if catalog is a db")
# pylint: disable=too-many-arguments
def cli(
    ctx,
    log_level,
    log_data,
    log_scan,
    catalog_format,
    catalog_file,
    catalog_host,
    catalog_port,
    catalog_user,
    catalog_password,
):
    logging.basicConfig(level=getattr(logging, log_level.upper()))
    logging.debug("Catalog - host: %s, port: %s, ", catalog_host, catalog_port)

    if log_scan:
        handler = logging.StreamHandler()
        handler.setFormatter(jsonlogger.JsonFormatter())
        handler.setLevel(logging.INFO)
        scan_logger.addHandler(handler)

    if log_data:
        handler = logging.StreamHandler()
        handler.setFormatter(jsonlogger.JsonFormatter())
        handler.setLevel(logging.INFO)
        data_logger.addHandler(handler)

    ctx.ensure_object(dict)

    ctx.obj["catalog"] = {
        "host": catalog_host,
        "port": catalog_port,
        "user": catalog_user,
        "password": catalog_password,
        "format": catalog_format,
        "file": catalog_file,
    }


cli.add_command(aws_cli)
cli.add_command(files_cli)
cli.add_command(sqlite_cli)
cli.add_command(db_cli)
cli.add_command(snowflake_cli)
