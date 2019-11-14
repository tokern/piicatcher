import argparse
import yaml
import logging

from piicatcher.db.aws import parser as aws_parser
from piicatcher.db.explorer import parser as db_parser
from piicatcher.files.explorer import parser as files_parser

from piicatcher.config import set_global_config
from piicatcher.orm.models import init


def get_parser(parser_cls=argparse.ArgumentParser):
    parser = parser_cls()
    parser.add_argument("-c", "--config-file", help="Path to config file")
    parser.add_argument("-l", "--log-level", help="Logging Level", default="WARNING")

    sub_parsers = parser.add_subparsers()
    aws_parser(sub_parsers)
    db_parser(sub_parsers)
    files_parser(sub_parsers)

    return parser


def dispatch(ns):
    logging.basicConfig(level=getattr(logging, ns.log_level.upper()))
    if ns.config_file is not None:
        with open(ns.config_file, 'r') as stream:
            set_global_config(yaml.load(stream))
            init()

    ns.func(ns)


def main():
    dispatch(get_parser().parse_args())
