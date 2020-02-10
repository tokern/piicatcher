from argparse import Namespace
from unittest import TestCase
from piicatcher.explorer.metadata import Column, Table, Schema
from piicatcher.piitypes import PiiTypes
from piicatcher.scanner import RegexScanner, NERScanner
from tests.test_models import MockExplorer


class DbMetadataTests(TestCase):
    data = {
        "no_pii": [
            ('abc', 'def'),
            ('xsfr', 'asawe')
        ],
        "partial_pii": [
            ('917-908-2234', 'plkj'),
            ('215-099-2234', 'sfrf')
        ],
        "full_pii": [
            ('Jonathan Smith', 'Virginia'),
            ('Chase Ryan', 'Chennai')
        ]
    }

    @staticmethod
    def data_generator(schema_name, table_name, column_list):
        for row in DbMetadataTests.data[table_name.get_name()]:
            yield row

    def test_negative_scan_column(self):
        col = Column('col')
        col.scan('abc', [RegexScanner(), NERScanner()])
        self.assertFalse(col.has_pii())
        self.assertEqual({'pii_types': [], 'name': 'col'}, col.get_dict())

    def test_positive_scan_column(self):
        col = Column('col')
        col.scan('Jonathan Smith', [RegexScanner(), NERScanner()])
        self.assertTrue(col.has_pii())
        self.assertEqual({'pii_types': [PiiTypes.PERSON], 'name': 'col'}, col.get_dict())

    def test_null_scan_column(self):
        col = Column('col')
        col.scan(None, [RegexScanner(), NERScanner()])
        self.assertFalse(col.has_pii())
        self.assertEqual({'pii_types': [], 'name': 'col'}, col.get_dict())

    def test_no_pii_table(self):
        schema = Schema('public')
        table = Table(schema, 'no_pii')
        table.add_child(Column('a'))
        table.add_child(Column('b'))

        table.scan(self.data_generator)
        self.assertFalse(table.has_pii())
        self.assertEqual({
            'columns': [{'name': 'a', 'pii_types': []}, {'name': 'b', 'pii_types': []}],
            'has_pii': False,
            'name': 'no_pii'}, table.get_dict())

    def test_partial_pii_table(self):
        schema = Schema('public')
        table = Table(schema, 'partial_pii')
        table.add_child(Column('a'))
        table.add_child(Column('b'))

        table.scan(self.data_generator)
        self.assertTrue(table.has_pii())
        cols = table.get_children()
        self.assertTrue(cols[0].has_pii())
        self.assertFalse(cols[1].has_pii())
        self.assertEqual({
            'columns': [{'name': 'a', 'pii_types': [PiiTypes.PHONE]},
                        {'name': 'b', 'pii_types': []}],
            'has_pii': True,
            'name': 'partial_pii'}, table.get_dict())

    def test_full_pii_table(self):
        schema = Schema('public')
        table = Table(schema, 'full_pii')
        table.add_child(Column('name'))
        table.add_child(Column('location'))

        table.scan(self.data_generator)
        self.assertTrue(table.has_pii())

        cols = table.get_children()
        self.assertTrue(cols[0].has_pii())
        self.assertTrue(cols[1].has_pii())
        self.assertEqual({
            'columns': [{'name': 'name', 'pii_types': [PiiTypes.PERSON]},
                        {'name': 'location', 'pii_types': [PiiTypes.LOCATION]}],
            'has_pii': True,
            'name': 'full_pii'}, table.get_dict())


class ShallowScan(TestCase):
    def test_no_pii_table(self):
        schema = Schema('public')
        table = Table(schema, 'no_pii')
        table.add_child(Column('a'))
        table.add_child(Column('b'))

        table.shallow_scan()
        self.assertFalse(table.has_pii())

    def test_partial_pii_table(self):
        schema = Schema('public')
        table = Table(schema, 'partial_pii')
        table.add_child(Column('fname'))
        table.add_child(Column('b'))

        table.shallow_scan()
        self.assertTrue(table.has_pii())
        cols = table.get_children()
        self.assertTrue(cols[0].has_pii())
        self.assertFalse(cols[1].has_pii())

    def test_full_pii_table(self):
        schema = Schema('public')
        table = Table(schema, 'full_pii')
        table.add_child(Column('name'))
        table.add_child(Column('dob'))

        table.shallow_scan()
        self.assertTrue(table.has_pii())

        cols = table.get_children()
        self.assertTrue(cols[0].has_pii())
        self.assertTrue(cols[1].has_pii())


class IncludeExcludeTests(TestCase):
    explorer = None

    def setUp(self):
        self.explorer = MockExplorer(Namespace(catalog=None,
                                               include_schema=(),
                                               exclude_schema=(),
                                               include_table=(),
                                               exclude_table=()
                                               ))
        self.explorer._load_catalog()

    def test_simple_schema_get(self):
        self.assertCountEqual(["test_store"], [s.get_name() for s in self.explorer.database.get_children()])

    def test_simple_schema_include(self):
        self.explorer.database.set_include_regex(['test_store'])
        self.assertCountEqual(["test_store"], [s.get_name() for s in self.explorer.database.get_children()])

    def test_simple_schema_exclude(self):
        self.explorer.database.set_exclude_regex(['test_store'])
        self.assertEqual(0, len([s.get_name() for s in self.explorer.database.get_children()]))

    def test_simple_schema_include_exclude(self):
        self.explorer.database.set_include_regex(['test_store'])
        self.explorer.database.set_exclude_regex(['test_store'])
        self.assertEqual(0, len([s.get_name() for s in self.explorer.database.get_children()]))

    def test_regex_schema_include(self):
        self.explorer.database.set_include_regex(['test_.*'])
        self.assertCountEqual(["test_store"], [s.get_name() for s in self.explorer.database.get_children()])

    def test_regex_schema_exclude(self):
        self.explorer.database.set_exclude_regex(['test_.*'])
        self.assertEqual(0, len([s.get_name() for s in self.explorer.database.get_children()]))

    def test_regex_failed_schema_include(self):
        self.explorer.database.set_include_regex(['tet_.*'])
        self.assertEqual(0, len([s.get_name() for s in self.explorer.database.get_children()]))

    def test_regex_failed_schema_exclude(self):
        self.explorer.database.set_exclude_regex(['tet_.*'])
        self.assertCountEqual(["test_store"], [s.get_name() for s in self.explorer.database.get_children()])

    def test_regex_success_table_include(self):
        schema = self.explorer.database.get_children()[0]
        schema.set_include_regex(["full.*", "partial.*"])

        self.assertCountEqual(["partial_pii", "full_pii"], [t.get_name() for t in schema.get_children()])

    def test_regex_success_table_exclude(self):
        schema = self.explorer.database.get_children()[0]
        schema.set_exclude_regex(["full.*", "partial.*"])

        self.assertCountEqual(["no_pii"], [t.get_name() for t in schema.get_children()])

    def test_regex_success_table_include_exclude(self):
        schema = self.explorer.database.get_children()[0]
        schema.set_include_regex(["full.*", "partial.*"])
        schema.set_exclude_regex(["full_pii"])

        self.assertCountEqual(["partial_pii"], [t.get_name() for t in schema.get_children()])
