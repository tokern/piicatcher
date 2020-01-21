from unittest import TestCase
from piicatcher.explorer.metadata import Column, Table, Schema
from piicatcher.piitypes import PiiTypes
from piicatcher.scanner import RegexScanner, NERScanner


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
        table.add(Column('a'))
        table.add(Column('b'))

        table.scan(self.data_generator)
        self.assertFalse(table.has_pii())
        self.assertEqual({
            'columns': [{'name': 'a', 'pii_types': []}, {'name': 'b', 'pii_types': []}],
            'has_pii': False,
            'name': 'no_pii'}, table.get_dict())

    def test_partial_pii_table(self):
        schema = Schema('public')
        table = Table(schema, 'partial_pii')
        table.add(Column('a'))
        table.add(Column('b'))

        table.scan(self.data_generator)
        self.assertTrue(table.has_pii())
        cols = table.get_columns()
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
        table.add(Column('name'))
        table.add(Column('location'))

        table.scan(self.data_generator)
        self.assertTrue(table.has_pii())

        cols = table.get_columns()
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
        table.add(Column('a'))
        table.add(Column('b'))

        table.shallow_scan()
        self.assertFalse(table.has_pii())

    def test_partial_pii_table(self):
        schema = Schema('public')
        table = Table(schema, 'partial_pii')
        table.add(Column('fname'))
        table.add(Column('b'))

        table.shallow_scan()
        self.assertTrue(table.has_pii())
        cols = table.get_columns()
        self.assertTrue(cols[0].has_pii())
        self.assertFalse(cols[1].has_pii())

    def test_full_pii_table(self):
        schema = Schema('public')
        table = Table(schema, 'full_pii')
        table.add(Column('name'))
        table.add(Column('dob'))

        table.shallow_scan()
        self.assertTrue(table.has_pii())

        cols = table.get_columns()
        self.assertTrue(cols[0].has_pii())
        self.assertTrue(cols[1].has_pii())

