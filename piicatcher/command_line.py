import argparse
import tableprint

from piicatcher.db.explorer import SqliteExplorer, MySQLExplorer, PostgreSQLExplorer


def get_parser(parser_cls=argparse.ArgumentParser):
    parser = parser_cls()
    parser.add_argument("-s", "--host", required=True,
                        help="Hostname of the database. File path if it is SQLite")
    parser.add_argument("-R", "--port",
                        help="Port of database.")
    parser.add_argument("-u", "--user",
                        help="Username to connect database")
    parser.add_argument("-p", "--password",
                        help="Password of the user")

    parser.add_argument("-t", "--connection-type", default="sqlite",
                        choices=["sqlite", "mysql", "postgres"],
                        help="Type of database")

    parser.add_argument("-c", "--scan-type", default='deep',
                        choices=["deep", "shallow"],
                        help="Choose deep(scan data) or shallow(scan column names only)")

    parser.add_argument("-o", "--output", default=None,
                        help="File path for report. If not specified, "
                             "then report is printed to sys.stdout")
    parser.add_argument("-f", "--output-format", choices=["ascii_table"],
                        default="ascii_table",
                        help="Choose output format type")

    return parser


def dispatch(ns):
    explorer = None
    if ns.connection_type == "sqlite":
        explorer = SqliteExplorer(ns.host)
    elif ns.connection_type == "mysql":
        explorer = MySQLExplorer(ns.host, ns.port, ns.user, ns.password)
    elif ns.connection_type == "postgres":
        explorer = PostgreSQLExplorer(ns.host, ns.port, ns.user, ns.password)

    assert(explorer is not None)

    if ns.scan_type is None or ns.scan_type == "deep":
        explorer.scan()
    else:
        explorer.shallow_scan()

    if ns.output_format == "ascii_table":
        headers = ["schema", "table", "column", "has_pii"]
        tableprint.table(explorer.get_tabular(), headers)


def main():
    dispatch(get_parser().parse_args())
