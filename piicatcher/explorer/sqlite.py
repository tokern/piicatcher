import logging
import sqlite3
from argparse import Namespace

import click

from piicatcher.explorer.explorer import Explorer


@click.command('sqlite')
@click.pass_context
@click.option("-s", "--path", required=True, help="File path to SQLite database")
@click.option("-f", "--output-format", type=click.Choice(["ascii_table", "json", "db"]),
              default="ascii_table",
              help="Choose output format type")
@click.option("-c", "--scan-type", default='shallow',
              type=click.Choice(["deep", "shallow"]),
              help="Choose deep(scan data) or shallow(scan column names only)")
@click.option("-o", "--output", default=None, type=click.File(),
              help="File path for report. If not specified, "
                   "then report is printed to sys.stdout")
@click.option("--list-all", default=False, is_flag=True,
              help="List all columns. By default only columns with PII information is listed")
def cli(cxt, path, output_format, scan_type, output, list_all):
    ns = Namespace(path=path,
                   output_format=output_format,
                   scan_type=scan_type,
                   output=output,
                   list_all=list_all,
                   catalog=cxt.obj['catalog'])

    SqliteExplorer.dispatch(ns)


class SqliteExplorer(Explorer):

    _catalog_query = """
            SELECT 
                "" as schema_name,
                m.name as table_name, 
                p.name as column_name,
                p.type as data_type
            FROM 
                sqlite_master AS m
            JOIN 
                pragma_table_info(m.name) AS p
            WHERE
                p.type like 'text' or p.type like 'varchar%' or p.type like 'char%'
            ORDER BY 
                m.name, 
                p.name
    """

    _query_template = "select {column_list} from {table_name}"
    _count_query = "select count(*) from {table_name}"

    @classmethod
    def factory(cls, ns):
        return SqliteExplorer(ns)

    class CursorContextManager:
        def __init__(self, connection):
            self.connection = connection

        def __enter__(self):
            self.cursor = self.connection.cursor()
            return self.cursor

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    def __init__(self, ns):
        super(SqliteExplorer, self).__init__(ns)
        self.path = ns.path

    @classmethod
    def factory(cls, ns):
        logging.debug("Sqlite Factory entered")
        return SqliteExplorer(ns)

    def _open_connection(self):
        logging.debug("Sqlite connection string '{}'".format(self.path))
        return sqlite3.connect(self.path)

    def _get_catalog_query(self):
        return self._catalog_query

    def _get_context_manager(self):
        return SqliteExplorer.CursorContextManager(self.get_connection())

    @classmethod
    def _get_select_query(cls, schema_name, table_name, column_list):
        return cls._query_template.format(
            column_list=",".join([col.get_name() for col in column_list]),
            table_name=table_name.get_name()
        )