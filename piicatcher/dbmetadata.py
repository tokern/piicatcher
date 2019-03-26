from abc import ABC, abstractmethod
import logging

from piicatcher.scanner import RegexScanner, NERScanner


class NamedObject(ABC):
    def __init__(self, name):
        self._name = name
        self._pii = set()

    def get_name(self):
        return self._name

    def has_pii(self):
        logging.debug("has_pii {} has {}".format(self, self._pii))
        return bool(self._pii)

    def get_pii_types(self):
        return self._pii

    @abstractmethod
    def scan(self, context):
        pass


class Schema(NamedObject):
    def __init__(self, name):
        super(Schema, self).__init__(name)
        self.tables = []

    def add(self, table):
        self.tables.append(table)

    def get_tables(self):
        return self.tables

    def scan(self, context):
        for table in self.tables:
            table.scan(context)
            logging.debug("{} has {}".format(table.get_name(), table.get_pii_types()))
            [self._pii.add(p) for p in table.get_pii_types()]

        logging.debug("{} has {}".format(self, self._pii))


class Table(NamedObject):
    query_template = "select {column_list} from `{schema_name}`.`{table_name}`"

    def __init__(self, schema, name):
        super(Table, self).__init__(name)
        self._schema = schema
        self.columns = []

    def add(self, col):
        self.columns.append(col)

    def get_columns(self):
        return self.columns

    def scan(self, context):
        query = self.query_template.format(
            column_list=",".join([col.get_name() for col in self.columns]),
            schema_name=self._schema.get_name(),
            table_name=self.get_name()
        )
        logging.debug(query)
        context.execute(query)
        row = context.fetchone()
        while row is not None:
            for col, val in zip(self.columns, row):
                col.scan(val)
            row = context.fetchone()

        for col in self.columns:
            [self._pii.add(p) for p in col.get_pii_types()]

        logging.debug(self._pii)


class Column(NamedObject):
    def __init__(self, name):
        super(Column, self).__init__(name)

    def scan(self, context):
        for scanner in [RegexScanner(), NERScanner()]:
            [self._pii.add(pii) for pii in scanner.scan(context)]

        logging.debug(self._pii)
