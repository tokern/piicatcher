import argparse
import logging

from piicatcher.explorer.aws import AthenaExplorer
from piicatcher.explorer.databases import RelDbExplorer
from piicatcher.explorer.files import parser as files_parser
from piicatcher.explorer.sqlite import SqliteExplorer


def get_parser(parser_cls=argparse.ArgumentParser):
    parser = parser_cls()
    parser.add_argument("-c", "--config-file", help="Path to config file")
    parser.add_argument("-l", "--log-level", help="Logging Level", default="WARNING")

    sub_parsers = parser.add_subparsers()
    AthenaExplorer.parser(sub_parsers)
    RelDbExplorer.parser(sub_parsers)
    SqliteExplorer.parser(sub_parsers)
    files_parser(sub_parsers)

    return parser


def dispatch(ns):
    logging.basicConfig(level=getattr(logging, ns.log_level.upper()))

    ns.func(ns)


def main():
    dispatch(get_parser().parse_args())
