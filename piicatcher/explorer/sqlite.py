import logging
import sqlite3
from argparse import Namespace

import click

from piicatcher.explorer.databases import (
    exclude_schema_help_text,
    exclude_table_help_text,
    schema_help_text,
    table_help_text,
)
from piicatcher.explorer.explorer import Explorer


@click.command("sqlite")
@click.pass_context
@click.option("-s", "--path", required=True, help="File path to SQLite database")
@click.option(
    "-c",
    "--scan-type",
    default="shallow",
    type=click.Choice(["deep", "shallow"]),
    help="Choose deep(scan data) or shallow(scan column names only)",
)
@click.option(
    "--list-all",
    default=False,
    is_flag=True,
    help="List all columns. By default only columns with PII information is listed",
)
@click.option("-n", "--schema", multiple=True, help=schema_help_text)
@click.option("-N", "--exclude-schema", multiple=True, help=exclude_schema_help_text)
@click.option("-t", "--table", multiple=True, help=table_help_text)
@click.option("-T", "--exclude-table", multiple=True, help=exclude_table_help_text)
# pylint: disable=too-many-arguments
def cli(
    cxt, path, scan_type, list_all, schema, exclude_schema, table, exclude_table,
):
    args = Namespace(
        path=path,
        scan_type=scan_type,
        list_all=list_all,
        catalog=cxt.obj["catalog"],
        include_schema=schema,
        exclude_schema=exclude_schema,
        include_table=table,
        exclude_table=exclude_table,
    )

    SqliteExplorer.dispatch(args)


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

    class CursorContextManager:
        def __init__(self, connection):
            self.connection = connection
            self.cursor = None

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
        logging.debug("Sqlite connection string '%s'", self.path)
        return sqlite3.connect(self.path)

    def _get_catalog_query(self):
        return self._catalog_query

    def _get_context_manager(self):
        return SqliteExplorer.CursorContextManager(self.get_connection())

    @classmethod
    def _get_select_query(cls, schema_name, table_name, column_list):
        return cls._query_template.format(
            column_list='"{0}"'.format(
                '","'.join(col.get_name() for col in column_list)
            ),
            table_name=table_name.get_name(),
        )
