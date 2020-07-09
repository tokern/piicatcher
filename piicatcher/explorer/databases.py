from abc import abstractmethod
from argparse import Namespace

import click
import pymysql
import psycopg2
import cx_Oracle

import logging

from piicatcher.explorer.explorer import Explorer

schema_help_text = '''
Scan only schemas matching schema; When this option is not specified, all
non-system schemas in the target database will be dumped. Multiple schemas can
be selected by writing multiple -n switches. Also, the schema parameter is
interpreted as a regular expression, so multiple schemas can also be selected
by writing wildcard characters in the pattern. When using wildcards, be careful
to quote the pattern if needed to prevent the shell from expanding the wildcards;
'''
exclude_schema_help_text = '''
Do not scan any schemas matching the schema pattern. The pattern is interpreted
according to the same rules as for -n. -N can be given more than once to exclude
 schemas matching any of several patterns.

When both -n and -N are given, the behavior is to dump just the schemas that
match at least one -n switch but no -N switches. If -N appears without -n, then
schemas matching -N are excluded from what is otherwise a normal dump.")
'''

table_help_text = '''
Dump only tables matching table. Multiple tables can be selected by writing
multiple -t switches. Also, the table parameter is interpreted as a regular
expression, so multiple tables can also be selected by writing wildcard
characters in the pattern. When using wildcards, be careful to quote the pattern
 if needed to prevent the shell from expanding the wildcards.

The -n and -N switches have no effect when -t is used, because tables selected
by -t will be dumped regardless of those switches.
'''

exclude_table_help_text = '''
Do not dump any tables matching the table pattern. The pattern is interpreted
according to the same rules as for -t. -T can be given more than once to
exclude tables matching any of several patterns.

When both -t and -T are given, the behavior is to dump just the tables that
match at least one -t switch but no -T switches. If -T appears without -t, then
tables matching -T are excluded from what is otherwise a normal dump.
'''


@click.command('db')
@click.pass_context
@click.option("-s", "--host", required=True, help="Hostname of the database")
@click.option("-R", "--port", help="Port of database.")
@click.option("-u", "--user", help="Username to connect database")
@click.option("-p", "--password", help="Password of the user")
@click.option("-d", "--database", default='', help="Name of the database")
@click.option("--connection-type", default="mysql",
              type=click.Choice(["mysql", "postgres", "redshift", "oracle"]),
              help="Type of database")
@click.option("-f", "--output-format", type=click.Choice(["ascii_table", "json", "db"]),
              help="DEPRECATED. Please use --catalog-format")
@click.option("-c", "--scan-type", type=click.Choice(["deep", "shallow"]), default='shallow',
              help="Choose deep(scan data) or shallow(scan column names only)")
@click.option("-o", "--output", default=None, type=click.File(),
              help="DEPRECATED. Please use --catalog-file")
@click.option("--list-all", default=False, is_flag=True,
              help="List all columns. By default only columns with PII information is listed")
@click.option("-n", "--schema", multiple=True, help=schema_help_text)
@click.option("-N", "--exclude-schema", multiple=True, help=exclude_schema_help_text)
@click.option("-t", "--table", multiple=True, help=table_help_text)
@click.option("-T", "--exclude-table", multiple=True, help=exclude_table_help_text)
def cli(cxt, host, port, user, password, database, connection_type,
        output_format, scan_type, output, list_all,
        schema, exclude_schema, table, exclude_table):
    ns = Namespace(host=host,
                   port=int(port) if port is not None else None,
                   user=user,
                   password=password,
                   database=database,
                   connection_type=connection_type,
                   scan_type=scan_type,
                   list_all=list_all,
                   catalog=cxt.obj['catalog'],
                   include_schema=schema,
                   exclude_schema=exclude_schema,
                   include_table=table,
                   exclude_table=exclude_table)

    if output_format is not None or output is not None:
        logging.warning("--output-format and --output is deprecated. "
                        "Please use --catalog-format and --catalog-file")

    if output_format is not None:
        ns.catalog['format'] = output_format

    if output is not None:
        ns.catalog['file'] = output

    RelDbExplorer.dispatch(ns)


class RelDbExplorer(Explorer):
    def __init__(self, ns):
        super(RelDbExplorer, self).__init__(ns)
        self.host = ns.host
        self.user = ns.user
        self.password = ns.password
        self.port = int(ns.port) if 'port' in vars(ns) and ns.port is not None else self.default_port

    @property
    @abstractmethod
    def default_port(self):
        pass

    @classmethod
    def factory(cls, ns):
        logging.debug("Relational Db Factory entered")
        explorer = None
        if ns.connection_type == "mysql":
            explorer = MySQLExplorer(ns)
        elif ns.connection_type == "postgres" or ns.connection_type == "redshift":
            explorer = PostgreSQLExplorer(ns)
        elif ns.connection_type == "oracle":
            explorer = OracleExplorer(ns)
        assert (explorer is not None)

        return explorer


class MySQLExplorer(RelDbExplorer):
    _catalog_query = """
        SELECT
            TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
        FROM
            INFORMATION_SCHEMA.COLUMNS
        WHERE
            TABLE_SCHEMA NOT IN ('information_schema', 'performance_schema', 'sys', 'mysql')
            AND DATA_TYPE RLIKE 'char.*|varchar.*|text'
        ORDER BY table_schema, table_name, column_name
    """

    _sample_query_template = "select {column_list} from {schema_name}.{table_name} limit 10"

    def __init__(self, ns):
        super(MySQLExplorer, self).__init__(ns)
        self._mysql_database = ns.database if 'database' in vars(ns) and ns.database is not None else None

    @property
    def default_port(self):
        return 3306

    def _open_connection(self):
        return pymysql.connect(host=self.host,
                               port=self.port,
                               user=self.user,
                               password=self.password,
                               database=self._mysql_database)

    def _get_catalog_query(self):
        return self._catalog_query

    @classmethod
    def _get_sample_query(cls, schema_name, table_name, column_list):
        return cls._sample_query_template.format(
            column_list='"{0}"'.format('","'.join(col.get_name() for col in column_list)),
            schema_name=schema_name.get_name(),
            table_name=table_name.get_name()
        )


class PostgreSQLExplorer(RelDbExplorer):
    _catalog_query = """
        SELECT
            TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
        FROM
            INFORMATION_SCHEMA.COLUMNS
        WHERE
            TABLE_SCHEMA NOT IN ('information_schema', 'pg_catalog')
            AND DATA_TYPE SIMILAR TO '%char%|%text%'
        ORDER BY table_schema, table_name, column_name
    """

    _sample_query_template = "select {column_list} from {schema_name}.{table_name} TABLESAMPLE BERNOULLI (10)"

    def __init__(self, ns):
        super(PostgreSQLExplorer, self).__init__(ns)
        self._pg_database = ns.database if 'database' in vars(ns) and ns.database is not None else None

    @property
    def default_database(self):
        return "public"

    @property
    def default_port(self):
        return 5432

    def _open_connection(self):
        return psycopg2.connect(host=self.host,
                                port=self.port,
                                user=self.user,
                                password=self.password,
                                database=self._pg_database)

    def _get_catalog_query(self):
        return self._catalog_query

    @classmethod
    def _get_sample_query(cls, schema_name, table_name, column_list):
        return cls._sample_query_template.format(
            column_list='"{0}"'.format('","'.join(col.get_name() for col in column_list)),
            schema_name=schema_name.get_name(),
            table_name=table_name.get_name()
        )


class OracleExplorer(RelDbExplorer):
    _catalog_query = """
        SELECT
            '{db}', TABLE_NAME, COLUMN_NAME
        FROM
            USER_TAB_COLUMNS
        WHERE UPPER(DATA_TYPE) LIKE '%CHAR%'
        ORDER BY TABLE_NAME, COLUMN_ID
    """

    _sample_query_template = "select {column_list} from {table_name} sample(5)"
    _select_query_template = "select {column_list} from {table_name}"
    _count_query = "select count(*) from {table_name}"

    def __init__(self, ns):
        super(OracleExplorer, self).__init__(ns)
        self._oracle_database = ns.database

    @property
    def default_port(self):
        return 1521

    def _open_connection(self):
        return cx_Oracle.connect(self.user,
                                 self.password,
                                 "%s:%d/%s" % (self.host, self.port, self._oracle_database))

    def _get_catalog_query(self):
        return self._catalog_query.format(db=self._oracle_database)

    @classmethod
    def _get_select_query(cls, schema_name, table_name, column_list):
        return cls._select_query_template.format(
            column_list='"{0}"'.format('","'.join(col.get_name() for col in column_list)),
            table_name=table_name.get_name()
        )

    @classmethod
    def _get_sample_query(cls, schema_name, table_name, column_list):
        return cls._sample_query_template.format(
            column_list='"{0}"'.format('","'.join(col.get_name() for col in column_list)),
            table_name=table_name.get_name()
        )

    @classmethod
    def _get_count_query(cls, schema_name, table_name):
        return cls._count_query.format(
            table_name=table_name.get_name()
        )
