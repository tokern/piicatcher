import logging
import click
import click_config_file

from piicatcher.explorer.aws import cli as aws_cli
from piicatcher.explorer.databases import cli as db_cli
from piicatcher.explorer.files import cli as files_cli
from piicatcher.explorer.sqlite import cli as sqlite_cli
from piicatcher import __version__


@click.group()
@click.pass_context
@click.version_option(__version__)
@click_config_file.configuration_option()
@click.option("-l", "--log-level", help="Logging Level", default="WARNING")
@click.option("--orm-host", help="Hostname of the database. Use if output is a db")
@click.option("--orm-port", help="Port of database. Use if output is a db")
@click.option("--orm-user", help="Username to connect database.  Use if output is a db")
@click.option("--orm-password", help="Password of the user. Use if output is a db")
def cli(ctx, log_level, orm_host, orm_port, orm_user, orm_password):
    logging.basicConfig(level=getattr(logging, log_level.upper()))
    logging.debug("ORM - host: %s, port: %s, user: %s, password: %s" % (orm_host, orm_port, orm_user, orm_password))

    ctx.ensure_object(dict)

    ctx.obj['orm'] = {
        'host': orm_host,
        'port': orm_port,
        'user': orm_user,
        'password': orm_password,
    }


cli.add_command(aws_cli)
cli.add_command(files_cli)
cli.add_command(sqlite_cli)
cli.add_command(db_cli)
