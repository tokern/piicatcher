import argparse
import tableprint

from piicatcher.dbexplorer import SqliteExplorer


def get_parser(parser_cls=argparse.ArgumentParser):
    parser = parser_cls()
    parser.add_argument("-c", "--connection", required=True,
                        help="DB API 2.0 compatible database connection string")
    parser.add_argument("-t", "--conncetion-type", default="sqlite",
                        choices=["sqlite", "mysql"],
                        help="Type of database")

    parser.add_argument("-o", "--output", default=None,
                        help="File path for report. If not specified, "
                             "then report is printed to sys.stdout")
    parser.add_argument("-f", "--output-format", choices=["ascii_table"],
                        default="ascii_table",
                        help="Choose output format type")

    return parser


def dispatch(ns):
    explorer = SqliteExplorer(ns.connection)
    explorer.scan()
    if ns.output_format == "ascii_table":
        headers = ["schema", "table", "column", "has_pii"]
        tableprint.table(explorer.get_tabular(), headers)


def main():
    dispatch(get_parser().parse_args())
