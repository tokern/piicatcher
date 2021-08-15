from argparse import Namespace
from typing import Dict
from unittest import TestCase

from dbcat.catalog.models import PiiTypes

from piicatcher.explorer.explorer import Explorer
from piicatcher.explorer.metadata import Column
from piicatcher.explorer.metadata import Database as mdDatabase
from piicatcher.explorer.metadata import Schema, Table
from piicatcher.scanner import NERScanner, RegexScanner


class MockExplorer(Explorer):
    @property
    def type(self) -> str:
        pass

    @property
    def connection_parameters(self) -> Dict[str, str]:
        pass

    @classmethod
    def _get_sample_query(cls, schema_name, table_name, column_list):
        pass

    @classmethod
    def parser(cls, sub_parsers):
        pass

    def _open_connection(self):
        pass

    def _get_catalog_query(self):
        pass

    @staticmethod
    def get_no_pii_table():
        no_pii_table = Table("test_store", "no_pii")
        no_pii_a = Column("a")
        no_pii_b = Column("b")

        no_pii_table.add_child(no_pii_a)
        no_pii_table.add_child(no_pii_b)

        return no_pii_table

    @staticmethod
    def get_partial_pii_table():
        partial_pii_table = Table("test_store", "partial_pii")
        partial_pii_a = Column("a")
        partial_pii_a.add_pii_type(PiiTypes.PHONE)
        partial_pii_b = Column("b")

        partial_pii_table.add_child(partial_pii_a)
        partial_pii_table.add_child(partial_pii_b)

        return partial_pii_table

    @staticmethod
    def get_full_pii_table():
        full_pii_table = Table("test_store", "full_pii")
        full_pii_a = Column("a")
        full_pii_a.add_pii_type(PiiTypes.PHONE)
        full_pii_b = Column("b")
        full_pii_b.add_pii_type(PiiTypes.ADDRESS)
        full_pii_b.add_pii_type(PiiTypes.LOCATION)

        full_pii_table.add_child(full_pii_a)
        full_pii_table.add_child(full_pii_b)

        return full_pii_table

    def _load_catalog(self):
        schema = Schema(
            "test_store", include=self._include_table, exclude=self._exclude_table
        )
        schema.add_child(MockExplorer.get_no_pii_table())
        schema.add_child(MockExplorer.get_partial_pii_table())
        schema.add_child(MockExplorer.get_full_pii_table())

        self._database = mdDatabase(
            "database", include=self._include_schema, exclude=self._exclude_schema
        )
        self._database.add_child(schema)


class DbMetadataTests(TestCase):
    data = {
        "no_pii": [("abc", "def"), ("xsfr", "asawe")],
        "partial_pii": [("917-908-2234", "plkj"), ("215-099-2234", "sfrf")],
        "full_pii": [("Jonathan Smith", "Virginia"), ("Chase Ryan", "Chennai")],
    }

    @staticmethod
    def data_generator(schema_name, table_name, column_list):
        for row in DbMetadataTests.data[table_name.get_name()]:
            yield row

    def test_negative_scan_column(self):
        col = Column("col")
        col.scan("abc", [RegexScanner(), NERScanner()])
        self.assertFalse(col.has_pii())
        self.assertEqual({"pii_types": [], "name": "col"}, col.get_dict())

    def test_positive_scan_column(self):
        col = Column("col")
        col.scan("Jonathan Smith", [RegexScanner(), NERScanner()])
        self.assertTrue(col.has_pii())
        self.assertEqual(
            {"pii_types": [PiiTypes.PERSON], "name": "col"}, col.get_dict()
        )

    def test_null_scan_column(self):
        col = Column("col")
        col.scan(None, [RegexScanner(), NERScanner()])
        self.assertFalse(col.has_pii())
        self.assertEqual({"pii_types": [], "name": "col"}, col.get_dict())

    def test_no_pii_table(self):
        schema = Schema("public")
        table = Table(schema, "no_pii")
        table.add_child(Column("a"))
        table.add_child(Column("b"))

        table.scan(self.data_generator)
        self.assertFalse(table.has_pii())
        self.assertEqual(
            {
                "columns": [
                    {"name": "a", "pii_types": []},
                    {"name": "b", "pii_types": []},
                ],
                "has_pii": False,
                "name": "no_pii",
            },
            table.get_dict(),
        )

    def test_partial_pii_table(self):
        schema = Schema("public")
        table = Table(schema, "partial_pii")
        table.add_child(Column("a"))
        table.add_child(Column("b"))

        table.scan(self.data_generator)
        self.assertTrue(table.has_pii())
        cols = table.get_children()
        self.assertTrue(cols[0].has_pii())
        self.assertFalse(cols[1].has_pii())
        self.assertEqual(
            {
                "columns": [
                    {"name": "a", "pii_types": [PiiTypes.PHONE]},
                    {"name": "b", "pii_types": []},
                ],
                "has_pii": True,
                "name": "partial_pii",
            },
            table.get_dict(),
        )

    def test_full_pii_table(self):
        schema = Schema("public")
        table = Table(schema, "full_pii")
        table.add_child(Column("name"))
        table.add_child(Column("location"))

        table.scan(self.data_generator)
        self.assertTrue(table.has_pii())

        cols = table.get_children()
        self.assertTrue(cols[0].has_pii())
        self.assertTrue(cols[1].has_pii())
        self.assertEqual(
            {
                "columns": [
                    {"name": "name", "pii_types": [PiiTypes.PERSON]},
                    {"name": "location", "pii_types": [PiiTypes.LOCATION]},
                ],
                "has_pii": True,
                "name": "full_pii",
            },
            table.get_dict(),
        )


class ShallowScan(TestCase):
    def test_no_pii_table(self):
        schema = Schema("public")
        table = Table(schema, "no_pii")
        table.add_child(Column("a"))
        table.add_child(Column("b"))

        table.shallow_scan()
        self.assertFalse(table.has_pii())

    def test_partial_pii_table(self):
        schema = Schema("public")
        table = Table(schema, "partial_pii")
        table.add_child(Column("fname"))
        table.add_child(Column("b"))

        table.shallow_scan()
        self.assertTrue(table.has_pii())
        cols = table.get_children()
        self.assertTrue(cols[0].has_pii())
        self.assertFalse(cols[1].has_pii())

    def test_full_pii_table(self):
        schema = Schema("public")
        table = Table(schema, "full_pii")
        table.add_child(Column("name"))
        table.add_child(Column("dob"))

        table.shallow_scan()
        self.assertTrue(table.has_pii())

        cols = table.get_children()
        self.assertTrue(cols[0].has_pii())
        self.assertTrue(cols[1].has_pii())


class IncludeExcludeTests(TestCase):
    explorer = None

    def setUp(self):
        self.explorer = MockExplorer(
            Namespace(
                catalog=None,
                include_schema=(),
                exclude_schema=(),
                include_table=(),
                exclude_table=(),
            )
        )
        self.explorer._load_catalog()

    def test_simple_schema_get(self):
        self.assertCountEqual(
            ["test_store"],
            [s.get_name() for s in self.explorer.database.get_children()],
        )

    def test_simple_schema_include(self):
        self.explorer.database.set_include_regex(["test_store"])
        self.assertCountEqual(
            ["test_store"],
            [s.get_name() for s in self.explorer.database.get_children()],
        )

    def test_simple_schema_exclude(self):
        self.explorer.database.set_exclude_regex(["test_store"])
        self.assertEqual(
            0, len([s.get_name() for s in self.explorer.database.get_children()])
        )

    def test_simple_schema_include_exclude(self):
        self.explorer.database.set_include_regex(["test_store"])
        self.explorer.database.set_exclude_regex(["test_store"])
        self.assertEqual(
            0, len([s.get_name() for s in self.explorer.database.get_children()])
        )

    def test_regex_schema_include(self):
        self.explorer.database.set_include_regex(["test_.*"])
        self.assertCountEqual(
            ["test_store"],
            [s.get_name() for s in self.explorer.database.get_children()],
        )

    def test_regex_schema_exclude(self):
        self.explorer.database.set_exclude_regex(["test_.*"])
        self.assertEqual(
            0, len([s.get_name() for s in self.explorer.database.get_children()])
        )

    def test_regex_failed_schema_include(self):
        self.explorer.database.set_include_regex(["tet_.*"])
        self.assertEqual(
            0, len([s.get_name() for s in self.explorer.database.get_children()])
        )

    def test_regex_failed_schema_exclude(self):
        self.explorer.database.set_exclude_regex(["tet_.*"])
        self.assertCountEqual(
            ["test_store"],
            [s.get_name() for s in self.explorer.database.get_children()],
        )

    def test_regex_success_table_include(self):
        schema = self.explorer.database.get_children()[0]
        schema.set_include_regex(["full.*", "partial.*"])

        self.assertCountEqual(
            ["partial_pii", "full_pii"], [t.get_name() for t in schema.get_children()]
        )

    def test_regex_success_table_exclude(self):
        schema = self.explorer.database.get_children()[0]
        schema.set_exclude_regex(["full.*", "partial.*"])

        self.assertCountEqual(["no_pii"], [t.get_name() for t in schema.get_children()])

    def test_regex_success_table_include_exclude(self):
        schema = self.explorer.database.get_children()[0]
        schema.set_include_regex(["full.*", "partial.*"])
        schema.set_exclude_regex(["full_pii"])

        self.assertCountEqual(
            ["partial_pii"], [t.get_name() for t in schema.get_children()]
        )
