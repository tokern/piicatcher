from abc import ABC, abstractmethod
import logging

from piicatcher.scanner import RegexScanner, NERScanner, ColumnNameScanner


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

    def scan(self, generator):
        for table in self.tables:
            table.scan(generator)
            logging.debug("{} has {}".format(table.get_name(), table.get_pii_types()))
            [self._pii.add(p) for p in table.get_pii_types()]

        logging.debug("{} has {}".format(self, self._pii))

    def shallow_scan(self):
        for table in self.tables:
            table.shallow_scan()
            [self._pii.add(p) for p in table.get_pii_types()]

        logging.debug("{} has {}".format(self, self._pii))


class Table(NamedObject):
    def __init__(self, schema, name):
        super(Table, self).__init__(name)
        self._schema = schema
        self._columns = []

    def add(self, col):
        self._columns.append(col)

    def get_columns(self):
        return self._columns

    def scan(self, generator):
        for row in generator(
            column_list=self._columns,
            schema_name=self._schema,
            table_name=self
        ):
            for col, val in zip(self._columns, row):
                col.scan(val)

        for col in self._columns:
            [self._pii.add(p) for p in col.get_pii_types()]

        logging.debug(self._pii)

    def shallow_scan(self):
        for col in self._columns:
            col.shallow_scan()
            [self._pii.add(p) for p in col.get_pii_types()]


class Column(NamedObject):
    def __init__(self, name):
        super(Column, self).__init__(name)
        self.column_scanner = ColumnNameScanner()

    def scan(self, data):
        for scanner in [RegexScanner(), NERScanner()]:
            [self._pii.add(pii) for pii in scanner.scan(data)]

        logging.debug(self._pii)

    def shallow_scan(self):
        [self._pii.add(pii) for pii in self.column_scanner.scan(self._name)]

