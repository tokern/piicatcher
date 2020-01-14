from argparse import Namespace
from unittest import TestCase

from piicatcher.explorer.explorer import Explorer
from piicatcher.explorer.metadata import Column, Schema, Table
from piicatcher.piitypes import PiiTypes


class MockExplorer(Explorer):
    def _open_connection(self):
        pass

    def _get_catalog_query(self):
        pass

    @classmethod
    def parser(cls, sub_parsers):
        pass


class ExplorerTest(TestCase):
    def setUp(self):
        self.explorer = MockExplorer(Namespace(host="mock_connection", orm=None))

        col1 = Column('c1')
        col2 = Column('c2')
        col2._pii = [PiiTypes.LOCATION]

        schema = Schema('s1')
        table = Table(schema, 't1')
        table._columns = [col1, col2]

        schema = Schema('testSchema')
        schema.tables = [table]

        self.explorer._schemas = [schema]

    def test_tabular_all(self):
        self.assertEqual([
            ['testSchema', 't1', 'c1', False],
            ['testSchema', 't1', 'c2', True]
        ], self.explorer.get_tabular(True))

    def test_tabular_pii(self):
        self.assertEqual([
            ['testSchema', 't1', 'c2', True]
        ], self.explorer.get_tabular(False))

