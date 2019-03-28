from abc import ABC, abstractmethod
from datetime import datetime, timedelta
import sqlite3
import pymysql
import psycopg2
import logging

from piicatcher.dbmetadata import Schema, Table, Column


class Explorer(ABC):
    def __init__(self, conn_string):
        self.conn_string = conn_string
        self._connection = None
        self._schemas = None

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

    @abstractmethod
    def get_schemas(self):
        pass

    @abstractmethod
    def get_tables(self, schema_name):
        pass

    @abstractmethod
    def get_columns(self, schema_name, table_name):
        pass

    def scan(self):
        for schema in self.get_schemas():
            schema.scan(self.get_connection().cursor())

    def get_tabular(self):
        tabular = []
        for schema in self._schemas:
            for table in schema.get_tables():
                for column in table.get_columns():
                    tabular.append([schema.get_name(), table.get_name(),
                                    column.get_name(), column.has_pii()])

        return tabular


class SqliteExplorer(Explorer):
    pragma_query = """
            SELECT 
                m.name as table_name, 
                p.name as column_name
            FROM 
                sqlite_master AS m
            JOIN 
                pragma_table_info(m.name) AS p
            {where_clause}
            ORDER BY 
                m.name, 
                p.cid 
    """

    def __init__(self, conn_string):
        super(SqliteExplorer, self).__init__(conn_string)

    def _open_connection(self):
        logging.debug("Sqlite connection string '{}'".format(self.conn_string))
        return sqlite3.connect(self.conn_string)

    def get_schemas(self):
        if self._schemas is None:
            sch = Schema('main')
            sch.tables.extend(self.get_tables(sch))
            self._schemas = [sch]
        return self._schemas

    def get_tables(self, schema):
        query = self.pragma_query.format(where_clause="")
        logging.debug(query)
        result_set = self.get_connection().execute(query)
        tables = []
        row = result_set.fetchone()
        current_table = None
        while row is not None:
            if current_table is None:
                current_table = Table(schema, row[0])
            elif current_table.get_name() != row[0]:
                tables.append(current_table)
                current_table = Table(schema, row[0])
            current_table.add(Column(row[1]))

            row = result_set.fetchone()

        if current_table is not None:
            tables.append(current_table)

        return tables

    def get_columns(self, schema_name, table_name):
        query = self.pragma_query.format(
            where_clause="WHERE m.name = ?"
        )
        logging.debug(query)
        logging.debug(table_name)
        result_set = self.get_connection().execute(query, (table_name,))
        columns = []
        row = result_set.fetchone()
        while row is not None:
            columns.append(Column(row[1]))
            row = result_set.fetchone()

        return columns


class CachedExplorer(Explorer):

    def __init__(self):
        super(CachedExplorer, self).__init__("")
        self._cache_ts = None

    def _load_catalog(self):
        if self._cache_ts is None or self._cache_ts < datetime.now() - timedelta(minutes=10):
            with self.get_connection().cursor() as cursor:
                cursor.execute(self._catalog_query)
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
                    current_table.columns.append(Column(row[2]))

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
            if s.get_name() == schema_name:
                return s.tables
        raise ValueError("{} schema not found".format(schema_name))

    def get_columns(self, schema_name, table_name):
        self._load_catalog()
        tables = self.get_tables(schema_name)
        for t in tables:
            if t.get_name() == table_name:
                return t.columns

        raise ValueError("{} table not found".format(table_name))


class MySQLExplorer(CachedExplorer):
    _catalog_query = """
        SELECT 
            TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE 
        FROM 
            INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA NOT IN ('information_schema', 'performance_schema', 'sys', 'mysql')
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


class PostgreSQLExplorer(CachedExplorer):
    _catalog_query = """
        SELECT 
            TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE 
        FROM 
            INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA NOT IN ('information_schema', 'pg_catalog')
    """

    def __init__(self, host, user, password, database):
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

