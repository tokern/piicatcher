from unittest import TestCase, mock
from shutil import rmtree

import sqlite3
import logging
import pytest

from piicatcher.dbexplorer import SqliteExplorer
from piicatcher.dbmetadata import Schema, Table, Column
from piicatcher.piitypes import PiiTypes

logging.basicConfig(level=logging.DEBUG)


class ExplorerTest(TestCase):
    def setUp(self):
        self.explorer = SqliteExplorer("mock_connection")

        col1 = Column('c1')
        col2 = Column('c2')
        col2._pii = [PiiTypes.LOCATION]

        table = Table('t1')
        table.columns = [col1, col2]

        schema = Schema('testSchema')
        schema.tables = [table]

        self.explorer._schemas = [schema]

    def test_tabular(self):
        self.assertEqual([
            ['testSchema', 't1', 'c1', False],
            ['testSchema', 't1', 'c2', True]
        ], self.explorer.get_tabular())


pii_data_script = """
create table no_pii(a text, b text);
insert into no_pii values ('abc', 'def');
insert into no_pii values ('xsfr', 'asawe');

create table partial_pii(a text, b text);
insert into partial_pii values ('917-908-2234', 'plkj');
insert into partial_pii values ('215-099-2234', 'sfrf');

create table full_pii(name text, location text);
insert into full_pii values ('Jonathan Smith', 'Virginia');
insert into full_pii values ('Chase Ryan', 'Chennai');
"""


@pytest.mark.usefixtures("temp_sqlite")
@pytest.mark.skip(reason="Old version of sqlite in Travis")
class SqliteTest(TestCase):

    @pytest.fixture(scope="class")
    def temp_sqlite(self, request, tmpdir_factory):
        request.cls.temp_dir = tmpdir_factory.mktemp("sqlite_test")
        request.cls.sqlite_conn = request.cls.temp_dir.join("sqldb")

        self.conn = sqlite3.connect(str(request.cls.sqlite_conn))
        self.conn.executescript(pii_data_script)
        self.conn.commit()
        self.conn.close()

        def finalizer():
            rmtree(self.temp_dir)
            print("Deleted {}".format(str(self.temp_dir)))

        request.addfinalizer(finalizer)

    def setUp(self):
        self.explorer = SqliteExplorer(str(self.sqlite_conn))

    def tearDown(self):
        self.explorer.get_connection().close()

    def test_columns(self):
        names = [col.get_name() for col in self.explorer.get_columns("no_pii")]
        self.assertEqual(['a', 'b'], names)

    def test_tables(self):
        names = [tbl.get_name() for tbl in self.explorer.get_tables('')]
        self.assertEqual(sorted(['no_pii', 'partial_pii', 'full_pii']), sorted(names))

    def test_schema(self):
        names = [sch.get_name() for sch in self.explorer.get_schemas()]
        self.assertEqual(['main'], names)

    def test_negative_scan_column(self):
        col = Column('col')
        col.scan('abc')
        self.assertFalse(col.has_pii())

    def test_positive_scan_column(self):
        col = Column('col')
        col.scan('Jonathan Smith')
        self.assertTrue(col.has_pii())

    def test_no_pii_table(self):
        table = Table('no_pii')
        table.add(Column('a'))
        table.add(Column('b'))

        table.scan(self.explorer.get_connection())
        self.assertFalse(table.has_pii())

    def test_partial_pii_table(self):
        table = Table('partial_pii')
        table.add(Column('a'))
        table.add(Column('b'))

        table.scan(self.explorer.get_connection())
        self.assertTrue(table.has_pii())
        cols = table.get_columns()
        self.assertTrue(cols[0].has_pii())
        self.assertFalse(cols[1].has_pii())

    def test_full_pii_table(self):
        table = Table('full_pii')
        table.add(Column('name'))
        table.add(Column('location'))

        table.scan(self.explorer.get_connection())
        self.assertTrue(table.has_pii())
        cols = table.get_columns()
        self.assertTrue(cols[0].has_pii())
        self.assertTrue(cols[1].has_pii())

    def test_scan_dbexplorer(self):
        self.explorer.scan()
        schema = self.explorer.get_schemas()[0]
        self.assertTrue(schema.has_pii())
