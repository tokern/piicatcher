from unittest import TestCase
from shutil import rmtree

import sqlite3
import logging
import pytest

from piicatcher.dbexplorer import SqliteExplorer

logging.basicConfig(level=logging.DEBUG)


@pytest.mark.usefixtures("temp_sqlite")
class SqliteTest(TestCase):

    @pytest.fixture(scope="class")
    def temp_sqlite(self, request, tmpdir_factory):
        request.cls.temp_dir = tmpdir_factory.mktemp("sqlite_test")
        request.cls.sqlite_conn = request.cls.temp_dir.join("sqldb")

        conn = sqlite3.connect(str(request.cls.sqlite_conn))
        conn.execute("create table a (i int, j int)")
        conn.execute("create table b (c text, d double)")
        conn.commit()
        conn.close()

        def finalizer():
            rmtree(self.temp_dir)
            print("Deleted {}".format(str(self.temp_dir)))

        request.addfinalizer(finalizer)

    def setUp(self):
        print("Self: " + str(self.sqlite_conn))
        self.explorer = SqliteExplorer(str(self.sqlite_conn))

    def tearDown(self):
        self.explorer.connection.close()

    def test_schema(self):
        self.assertEqual(['main'], self.explorer.get_schemas())

    def test_columns(self):
        names = [col.name for col in self.explorer.get_columns('b')]
        self.assertEqual(['c', 'd'], names)

    def test_tables(self):
        names = [tbl.name for tbl in self.explorer.get_tables('')]
        self.assertEqual(['a', 'b'], names)
