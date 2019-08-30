import argparse
import yaml

from piicatcher.db.explorer import parser as explorer_parser
from piicatcher.config import set_global_config
from piicatcher.orm.models import init


def get_parser(parser_cls=argparse.ArgumentParser):
    parser = parser_cls()
    parser.add_argument("-c", "--config-file", help="Path to config file")

    sub_parsers = parser.add_subparsers()
    explorer_parser(sub_parsers)

    return parser


def dispatch(ns):
    if ns.config_file is not None:
        with open(ns.config_file, 'r') as stream:
            set_global_config(yaml.load(stream))
            init()


def main():
    dispatch(get_parser().parse_args())
