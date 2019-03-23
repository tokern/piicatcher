from abc import ABC, abstractmethod
import sqlite3
import logging

from piicatcher.dbmetadata import Schema, Table, Column


class Explorer(ABC):
    def __init__(self, conn_string):
        self.conn_string = conn_string
        self._connection = None
        self._schemas = None

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
    def get_tables(self, schema):
        pass

    @abstractmethod
    def get_columns(self, table):
        pass

    def scan(self):
        for schema in self.get_schemas():
            schema.scan(self.get_connection())

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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()

    def _open_connection(self):
        logging.debug("Sqlite connection string '{}'".format(self.conn_string))
        return sqlite3.connect(self.conn_string)

    def get_schemas(self):
        if self._schemas is None:
            sch = Schema('main')
            sch.tables.extend(self.get_tables('main'))
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
                current_table = Table(row[0])
            elif current_table.get_name() != row[0]:
                tables.append(current_table)
                current_table = Table(row[0])
            current_table.add(Column(row[1]))

            row = result_set.fetchone()

        if current_table is not None:
            tables.append(current_table)

        return tables

    def get_columns(self, table):
        query = self.pragma_query.format(
            where_clause="WHERE m.name = ?"
        )
        logging.debug(query)
        logging.debug(table)
        result_set = self.get_connection().execute(query, (table,))
        columns = []
        row = result_set.fetchone()
        while row is not None:
            columns.append(Column(row[1]))
            row = result_set.fetchone()

        return columns

