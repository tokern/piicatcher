import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta

import tableprint

from piicatcher.explorer.metadata import Schema, Table, Column
from piicatcher.store.db import DbStore


class Explorer(ABC):
    query_template = "select {column_list} from {schema_name}.{table_name}"
    _count_query = "select count(*) from {schema_name}.{table_name}"

    def __init__(self, ns):
        self._connection = None
        self._schemas = None
        self._cache_ts = None
        self.catalog = ns.catalog

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
        pass

    @property
    def small_table_max(self):
        return 100

    @classmethod
    def dispatch(cls, ns):
        logging.debug("Dispatch of %s" % cls.__name__)
        explorer = cls.factory(ns)
        if ns.scan_type is None or ns.scan_type == "deep":
            explorer.scan()
        else:
            explorer.shallow_scan()

        cls.output(ns, explorer)

    @classmethod
    def output(cls, ns, explorer):
        if ns.output_format == "ascii_table":
            headers = ["schema", "table", "column", "has_pii"]
            tableprint.table(explorer.get_tabular(ns.list_all), headers)
        elif ns.output_format == "json":
            print(json.dumps(explorer.get_dict(), sort_keys=True, indent=2))
        elif ns.output_format == "db":
            DbStore.save_schemas(explorer)

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
        raise NotImplementedError

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
        if count < self.small_table_max:
            query = self._get_select_query(schema_name, table_name, column_list)
        else:
            try:
                query = self._get_sample_query(schema_name, table_name, column_list)
            except NotImplementedError:
                logging.warning("Sample Row is not implemented for %s" % self.__class__.__name__)
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