import argparse
import logging


def get_parser(parser_cls=argparse.ArgumentParser):
    parser = parser_cls()
    parser.add_argument("-c", "--config-file", help="Path to config file")
    parser.add_argument("-l", "--log-level", help="Logging Level", default="WARNING")

    return parser


import click
import click_config_file

from piicatcher.explorer.aws import cli as aws_cli
from piicatcher.explorer.databases import cli as db_cli
from piicatcher.explorer.files import cli as files_cli
from piicatcher.explorer.sqlite import cli as sqlite_cli


@click.group()
@click.pass_context
@click_config_file.configuration_option()
@click.option("-l", "--log-level", help="Logging Level", default="WARNING")
def cli(ctx, log_level):
    logging.basicConfig(level=getattr(logging, log_level.upper()))


cli.add_command(aws_cli)
cli.add_command(files_cli)
cli.add_command(sqlite_cli)
cli.add_command(db_cli)
