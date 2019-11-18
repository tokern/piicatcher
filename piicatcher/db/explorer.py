from abc import ABC, abstractmethod
from datetime import datetime, timedelta

import sqlite3
import pymysql
import psycopg2
import pymssql
import cx_Oracle

import logging
import json
import tableprint

from piicatcher.db.metadata import Schema, Table, Column
from piicatcher.orm.models import Store


class Explorer(ABC):
    query_template = "select {column_list} from {schema_name}.{table_name}"
    _count_query = "select count(*) from {schema_name}.{table_name}"

    def __init__(self):
        self._connection = None
        self._schemas = None
        self._cache_ts = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()

    @abstractmethod
    def _open_connection(self):
        pass

    @abstractmethod
    def _get_catalog_query(self):
        pass

    @classmethod
    def factory(cls, ns):
        logging.debug("Db Dispatch entered")
        explorer = None
        if ns.connection_type == "sqlite":
            explorer = SqliteExplorer(ns.host)
        elif ns.connection_type == "mysql":
            explorer = MySQLExplorer(ns.host, ns.port, ns.user, ns.password)
        elif ns.connection_type == "postgres" or ns.connection_type == "redshift":
            explorer = PostgreSQLExplorer(ns.host, ns.port, ns.user, ns.password, ns.database)
        elif ns.connection_type == "sqlserver":
            explorer = MSSQLExplorer(ns.host, ns.port, ns.user, ns.password, ns.database)
        elif ns.connection_type == "oracle":
            explorer = OracleExplorer(ns.host, ns.port, ns.user, ns.password, ns.database)
        assert (explorer is not None)

        return explorer

    @classmethod
    def parser(cls, sub_parsers):
        sub_parser = sub_parsers.add_parser("db")

        sub_parser.add_argument("-s", "--host", required=True,
                                help="Hostname of the database. File path if it is SQLite")
        sub_parser.add_argument("-R", "--port",
                                help="Port of database.")
        sub_parser.add_argument("-u", "--user",
                                help="Username to connect database")
        sub_parser.add_argument("-p", "--password",
                                help="Password of the user")
        sub_parser.add_argument("-d", "--database", default='',
                                help="Name of the database")
        sub_parser.add_argument("-t", "--connection-type", default="sqlite",
                                choices=["sqlite", "mysql", "postgres", "redshift", "oracle", "sqlserver"],
                                help="Type of database")

        cls.scan_options(sub_parser)

    @classmethod
    def scan_options(cls, sub_parser):
        sub_parser.add_argument("-c", "--scan-type", default='shallow',
                                choices=["deep", "shallow"],
                                help="Choose deep(scan data) or shallow(scan column names only)")

        sub_parser.add_argument("-o", "--output", default=None,
                                help="File path for report. If not specified, "
                                     "then report is printed to sys.stdout")
        sub_parser.add_argument("-f", "--output-format", choices=["ascii_table", "json", "orm"],
                                default="ascii_table",
                                help="Choose output format type")
        sub_parser.add_argument("--list-all", action="store_true", default=False,
                                help="List all columns. By default only columns with PII information is listed")
        sub_parser.set_defaults(func=Explorer.dispatch)

    @classmethod
    def dispatch(cls, ns):
        explorer = cls.factory(ns)
        if ns.scan_type is None or ns.scan_type == "deep":
            explorer.scan()
        else:
            explorer.shallow_scan()

        if ns.output_format == "ascii_table":
            headers = ["schema", "table", "column", "has_pii"]
            tableprint.table(explorer.get_tabular(ns.list_all), headers)
        elif ns.output_format == "json":
            print(json.dumps(explorer.get_dict(), sort_keys=True, indent=2))
        elif ns.output_format == "orm":
            Store.save_schemas(explorer)

    def get_connection(self):
        if self._connection is None:
            self._connection = self._open_connection()
        return self._connection

    def close_connection(self):
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def scan(self):
        for schema in self.get_schemas():
            schema.scan(self._generate_rows)

    def shallow_scan(self):
        for schema in self.get_schemas():
            schema.shallow_scan()

    def get_tabular(self, list_all):
        tabular = []
        for schema in self._schemas:
            for table in schema.get_tables():
                for column in table.get_columns():
                    if list_all or column.has_pii():
                        tabular.append([schema.get_name(), table.get_name(),
                                       column.get_name(), column.has_pii()])

        return tabular

    def get_dict(self):
        schemas = []
        for schema in self._schemas:
            schemas.append(schema.get_dict())

        return schemas

    @classmethod
    def _get_count_query(cls, schema_name, table_name):
        return cls._count_query.format(
            schema_name=schema_name.get_name(),
            table_name=table_name.get_name()
        )

    @classmethod
    def _get_select_query(cls, schema_name, table_name, column_list):
        return cls.query_template.format(
            column_list=",".join([col.get_name() for col in column_list]),
            schema_name=schema_name.get_name(),
            table_name=table_name.get_name()
        )

    @classmethod
    def _get_sample_query(cls, schema_name, table_name, column_list):
        return NotImplementedError

    def _get_table_count(self, schema_name, table_name, column_list):
        count = self._get_count_query(schema_name, table_name)
        logging.debug("Count Query: %s" % count)

        with self._get_context_manager() as cursor:
            cursor.execute(count)
            row = cursor.fetchone()

            return int(row[0])

    def _get_query(self, schema_name, table_name, column_list):
        count = self._get_table_count(schema_name, table_name, column_list)
        query = None
        if count < 100:
            query = self._get_select_query(schema_name, table_name, column_list)
        else:
            try:
                query = self._get_sample_query(schema_name, table_name, column_list)
            except NotImplementedError:
                query = self._get_select_query(schema_name, table_name, column_list)

        return query

    def _generate_rows(self, schema_name, table_name, column_list):
        query = self._get_query(schema_name, table_name, column_list)
        logging.debug(query)
        with self._get_context_manager() as cursor:
            cursor.execute(query)
            row = cursor.fetchone()
            while row is not None:
                yield row
                row = cursor.fetchone()

    def _get_context_manager(self):
        return self.get_connection().cursor()

    def _load_catalog(self):
        if self._cache_ts is None or self._cache_ts < datetime.now() - timedelta(minutes=10):
            with self._get_context_manager() as cursor:
                logging.debug("Catalog Query: %s", self._get_catalog_query())
                cursor.execute(self._get_catalog_query())
                self._schemas = []

                row = cursor.fetchone()

                current_schema = None
                current_table = None

                if row is not None:
                    current_schema = Schema(row[0])
                    current_table = Table(current_schema, row[1])

                while row is not None:
                    if current_schema.get_name() != row[0]:
                        current_schema.tables.append(current_table)
                        self._schemas.append(current_schema)
                        current_schema = Schema(row[0])
                        current_table = Table(current_schema, row[1])
                    elif current_table.get_name() != row[1]:
                        current_schema.tables.append(current_table)
                        current_table = Table(current_schema, row[1])
                    current_table.add(Column(row[2]))

                    row = cursor.fetchone()

                if current_schema is not None and current_table is not None:
                    current_schema.tables.append(current_table)
                    self._schemas.append(current_schema)

            self._cache_ts = datetime.now()

    def get_schemas(self):
        self._load_catalog()
        return self._schemas

    def get_tables(self, schema_name):
        self._load_catalog()
        for s in self._schemas:
            print(schema_name)
            print(s.get_name())
            if s.get_name() == schema_name:
                return s.tables
        raise ValueError("{} schema not found".format(schema_name))

    def get_columns(self, schema_name, table_name):
        self._load_catalog()
        tables = self.get_tables(schema_name)
        for t in tables:
            if t.get_name() == table_name:
                return t.get_columns()

        raise ValueError("{} table not found".format(table_name))


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

    class CursorContextManager():
        def __init__(self, connection):
            self.connection = connection

        def __enter__(self):
            self.cursor = self.connection.cursor()
            return self.cursor

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    def __init__(self, conn_string):
        super(SqliteExplorer, self).__init__()
        self.conn_string = conn_string

    def _open_connection(self):
        logging.debug("Sqlite connection string '{}'".format(self.conn_string))
        return sqlite3.connect(self.conn_string)

    def _get_catalog_query(self):
        return self._catalog_query

    def _get_context_manager(self):
        return SqliteExplorer.CursorContextManager(self.get_connection())

    @classmethod
    def _get_select_query(cls, schema_name, table_name, column_list):
        return self._query_template.format(
            column_list=",".join([col.get_name() for col in column_list]),
            table_name=table_name.get_name()
        )


class MySQLExplorer(Explorer):
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

    default_port = 3036

    def __init__(self, host, port, user, password):
        super(MySQLExplorer, self).__init__()
        self.host = host
        self.user = user
        self.password = password
        self.port = self.default_port if port is None else int(port)

    def _open_connection(self):
        return pymysql.connect(host=self.host,
                               port=self.port,
                               user=self.user,
                               password=self.password)

    def _get_catalog_query(self):
        return self._catalog_query


class PostgreSQLExplorer(Explorer):
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

    default_port = 5432

    def __init__(self, host, port, user, password, database='public'):
        super(PostgreSQLExplorer, self).__init__()
        self.host = host
        self.port = self.default_port if port is None else int(port)
        self.user = user
        self.password = password
        self.database = database

    def _open_connection(self):
        return psycopg2.connect(host=self.host,
                                port=self.port,
                                user=self.user,
                                password=self.password,
                                database=self.database)

    def _get_catalog_query(self):
        return self._catalog_query


class MSSQLExplorer(Explorer):
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
    default_port = 1433

    def __init__(self, host, port, user, password, database='public'):
        super(MSSQLExplorer, self).__init__()
        self.host = host
        self.port = self.default_port if port is None else int(port)
        self.user = user
        self.password = password
        self.database = database

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


class OracleExplorer(Explorer):
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

    default_port = 1521

    def __init__(self, host, port, user, password, database):
        super(OracleExplorer, self).__init__()
        self.host = host
        self.port = self.default_port if port is None else int(port)
        self.user = user
        self.password = password
        self.database = database

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
