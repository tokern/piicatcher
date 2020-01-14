from abc import abstractmethod
from argparse import Namespace

import click
import pymysql
import psycopg2
import pymssql
import cx_Oracle

import logging

from piicatcher.explorer.explorer import Explorer


@click.command('db')
@click.pass_context
@click.option("-s", "--host", required=True, help="Hostname of the database")
@click.option("-R", "--port", help="Port of database.")
@click.option("-u", "--user", help="Username to connect database")
@click.option("-p", "--password", help="Password of the user")
@click.option("-d", "--database", default='', help="Name of the database")
@click.option("-t", "--connection-type", default="mysql",
              type=click.Choice(["mysql", "postgres", "redshift", "oracle", "sqlserver"]),
              help="Type of database")
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
def cli(cxt, host, port, user, password, database, connection_type, output_format, scan_type, output, list_all):
    ns = Namespace(host=host,
                   port=int(port) if port is not None else None,
                   user=user,
                   password=password,
                   database=database,
                   connection_type=connection_type,
                   output_format=output_format,
                   scan_type=scan_type,
                   output=output,
                   list_all=list_all,
                   orm=cxt.obj['orm'])

    logging.info(vars(ns))
    Explorer.dispatch(ns)


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
        elif ns.connection_type == "sqlserver":
            explorer = MSSQLExplorer(ns)
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
        self.database = ns.database if 'database' in vars(ns) and ns.database is not None else None

    @property
    def default_port(self):
        return 3306

    def _open_connection(self):
        return pymysql.connect(host=self.host,
                               port=self.port,
                               user=self.user,
                               password=self.password,
                               database=self.database)

    def _get_catalog_query(self):
        return self._catalog_query

    @classmethod
    def _get_sample_query(cls, schema_name, table_name, column_list):
        return cls.query_template.format(
            column_list=",".join([col.get_name() for col in column_list]),
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
        self.database = ns.database if 'database' in vars(ns) and ns.database is not None else None

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
                                database=self.database)

    def _get_catalog_query(self):
        return self._catalog_query

    @classmethod
    def _get_sample_query(cls, schema_name, table_name, column_list):
        return cls.query_template.format(
            column_list=",".join([col.get_name() for col in column_list]),
            schema_name=schema_name.get_name(),
            table_name=table_name.get_name()
        )


class MSSQLExplorer(RelDbExplorer):
    _catalog_query = """
        SELECT 
            TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE 
        FROM 
            INFORMATION_SCHEMA.COLUMNS 
        WHERE 
            DATA_TYPE LIKE '%char%'
        ORDER BY TABLE_SCHEMA, table_name, ordinal_position 
    """

    _sample_query_template = "SELECT TOP 10 * FROM {schema_name}.{table_name} TABLESAMPLE (1000 ROWS)"

    def __init__(self, ns):
        super(MSSQLExplorer, self).__init__(ns)
        self.database = self.database if ns.database is None else ns.database

    @property
    def default_database(self):
        return "public"

    @property
    def default_port(self):
        return 1433

    def _open_connection(self):
        return pymssql.connect(host=self.host,
                               port=self.port,
                               user=self.user,
                               password=self.password,
                               database=self.database)

    def _get_catalog_query(self):
        return self._catalog_query

    @classmethod
    def _get_sample_query(cls, schema_name, table_name, column_list):
        return cls._sample_query_template.format(
            column_list=",".join([col.get_name() for col in column_list]),
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
        self.database = ns.database

    @property
    def default_port(self):
        return 1521

    def _open_connection(self):
        return cx_Oracle.connect(self.user,
                                 self.password,
                                 "%s:%d/%s" % (self.host, self.port, self.database))

    def _get_catalog_query(self):
        return self._catalog_query.format(db=self.database)

    @classmethod
    def _get_select_query(cls, schema_name, table_name, column_list):
        return cls._select_query_template.format(
            column_list=",".join([col.get_name() for col in column_list]),
            table_name=table_name.get_name()
        )

    @classmethod
    def _get_sample_query(cls, schema_name, table_name, column_list):
        return cls._sample_query_template.format(
            column_list=",".join([col.get_name() for col in column_list]),
            table_name=table_name.get_name()
        )

    @classmethod
    def _get_count_query(cls, schema_name, table_name):
        return cls._count_query.format(
            table_name=table_name.get_name()
        )
