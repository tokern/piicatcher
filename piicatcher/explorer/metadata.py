from abc import ABC, abstractmethod
import logging

from piicatcher.scanner import RegexScanner, NERScanner, ColumnNameScanner


class NamedObject(ABC):
    def __init__(self, name):
        self._name = name
        self._pii = set()
        self._children = []

    def get_name(self):
        return self._name

    def has_pii(self):
        logging.debug("has_pii {} has {}".format(self, self._pii))
        return bool(self._pii)

    def get_pii_types(self):
        return self._pii

    def get_children(self):
        return self._children

    def add_child(self, child):
        self._children.append(child)

    def scan(self, generator):
        for child in self.get_children():
            child.scan(generator)
            logging.debug("{} has {}".format(child.get_name(), child.get_pii_types()))
            [self._pii.add(p) for p in child.get_pii_types()]

        logging.debug("{} has {}".format(self, self._pii))

    def shallow_scan(self):
        for child in self.get_children():
            child.shallow_scan()
            [self._pii.add(p) for p in child.get_pii_types()]

        logging.debug("{} has {}".format(self, self._pii))


class Database(NamedObject):
    def __init__(self, name):
        super(Database, self).__init__(name)


class Schema(NamedObject):
    def __init__(self, name):
        super(Schema, self).__init__(name)

    def get_dict(self):
        dict = {
            'has_pii': self.has_pii(),
            'name': self._name,
            'tables': []
        }

        for table in self.get_children():
            dict['tables'].append(table.get_dict())

        return dict


class Table(NamedObject):
    def __init__(self, schema, name):
        super(Table, self).__init__(name)
        self._schema = schema

    def scan(self, generator):
        scanners = [
            RegexScanner(),
            NERScanner()
        ]
        for row in generator(
            column_list=self.get_children(),
            schema_name=self._schema,
            table_name=self
        ):
            for col, val in zip(self.get_children(), row):
                col.scan(val, scanners)

        for col in self.get_children():
            [self._pii.add(p) for p in col.get_pii_types()]

        logging.debug(self._pii)

    def shallow_scan(self):
        for col in self.get_children():
            col.shallow_scan()
            [self._pii.add(p) for p in col.get_pii_types()]

    def get_dict(self):
        dict = {
            'has_pii': self.has_pii(),
            'name': self.get_name(),
            'columns': []
        }

        for col in self.get_children():
            dict['columns'].append(col.get_dict())
        return dict


class Column(NamedObject):
    def __init__(self, name):
        super(Column, self).__init__(name)
        self.column_scanner = ColumnNameScanner()

    def add_pii_type(self, pii):
        self._pii.add(pii)

    def scan(self, data, scanners):
        if data is not None:
            for scanner in scanners:
                [self._pii.add(pii) for pii in scanner.scan(data)]

            logging.debug(self._pii)

    def shallow_scan(self):
        logging.debug("Scanning column name %s" % self._name)
        [self._pii.add(pii) for pii in self.column_scanner.scan(self._name)]

    def get_dict(self):
        return {
            'pii_types': list(self.get_pii_types()),
            'name': self.get_name()
        }
