from abc import ABC, abstractmethod
from datetime import datetime, timedelta
import sqlite3
import pymysql
import psycopg2
import logging

from piicatcher.dbmetadata import Schema, Table, Column


class Explorer(ABC):
    query_template = "select {column_list} from {schema_name}.{table_name}"

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

    def get_connection(self):
        if self._connection is None:
            self._connection = self._open_connection()
        return self._connection

    def close_connection(self):
        if self._connection is not None:
            self._connection.close()

    def scan(self):
        for schema in self.get_schemas():
            schema.scan(self._generate_rows)

    def get_tabular(self):
        tabular = []
        for schema in self._schemas:
            for table in schema.get_tables():
                for column in table.get_columns():
                    tabular.append([schema.get_name(), table.get_name(),
                                    column.get_name(), column.has_pii()])

        return tabular

    def _get_select_query(self, schema_name, table_name, column_list):
        return self.query_template.format(
            column_list=",".join([col.get_name() for col in column_list]),
            schema_name=schema_name.get_name(),
            table_name=table_name.get_name()
        )

    def _generate_rows(self, schema_name, table_name, column_list):
        query = self._get_select_query(schema_name, table_name, column_list)
        logging.debug(query)
        with self._get_context_manager() as cursor:
            cursor.execute(query)
            row = cursor.fetchone()
            while row is not None:
                yield row
                row = cursor.fetchone()

    @abstractmethod
    def _get_catalog_query(self):
        pass

    def _get_context_manager(self):
        return self.get_connection().cursor()

    def _load_catalog(self):
        if self._cache_ts is None or self._cache_ts < datetime.now() - timedelta(minutes=10):
            with self._get_context_manager() as cursor:
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

    def _get_select_query(self, schema_name, table_name, column_list):
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

    def __init__(self, host, user, password):
        super(MySQLExplorer, self).__init__()
        self.host = host
        self.user = user
        self.password = password

    def _open_connection(self):
        return pymysql.connect(host=self.host,
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

    def __init__(self, host, user, password, database='public'):
        super(PostgreSQLExplorer, self).__init__()
        self.host = host
        self.user = user
        self.password = password
        self.database = database

    def _open_connection(self):
        return psycopg2.connect(host=self.host,
                                user=self.user,
                                password=self.password,
                                database=self.database)

    def _get_catalog_query(self):
        return self._catalog_query
