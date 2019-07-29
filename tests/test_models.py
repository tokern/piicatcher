from unittest import TestCase
import sqlite3
import logging
import pytest

from piicatcher.orm.models import *
from piicatcher.db.explorer import SqliteExplorer
from piicatcher.piitypes import PiiTypes

logging.basicConfig(level=logging.DEBUG)


class TestCreateTables(TestCase):
    sqlite_path = 'file::memory:?cache=shared'

    def setUp(self):
        init_test(self.sqlite_path)
        self.explorer = SqliteExplorer(self.sqlite_path)

    def test_schemas_exist(self):
        self.assertEqual(1, len(self.explorer.get_schemas()))

    def test_tables_exist(self):
        schema = self.explorer.get_schemas()[0]

        self.assertEqual(["columns", "schemas", "tables"],
                         [t.get_name() for t in schema.get_tables()])


