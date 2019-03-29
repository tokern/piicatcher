from abc import ABC, abstractmethod
from unittest import TestCase
from shutil import rmtree

import sqlite3
import pymysql
import psycopg2
import logging
import pytest

from piicatcher.db.explorer import SqliteExplorer, MySQLExplorer, PostgreSQLExplorer
from piicatcher.db.metadata import Schema, Table, Column
from piicatcher.piitypes import PiiTypes

logging.basicConfig(level=logging.DEBUG)


class ExplorerTest(TestCase):
    def setUp(self):
        self.explorer = SqliteExplorer("mock_connection")

        col1 = Column('c1')
        col2 = Column('c2')
        col2._pii = [PiiTypes.LOCATION]

        schema = Schema('s1')
        table = Table(schema, 't1')
        table._columns = [col1, col2]

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

char_data_types = """
create table char_columns(ch char, vch varchar, chn char(10), cvhn varchar(10));
create table no_char_column(i int, d double, t time, dt date, ts timestamp);
"""


class CommonExplorerTestCases:
    class CommonExplorerTests(TestCase, ABC):
        @abstractmethod
        def get_test_schema(self):
            pass

        def test_columns(self):
            names = [col.get_name() for col in self.explorer.get_columns(self.get_test_schema(), "no_pii")]
            self.assertEqual(['a', 'b'], names)

        def test_tables(self):
            names = [tbl.get_name() for tbl in self.explorer.get_tables(self.get_test_schema())]
            self.assertEqual(sorted(['no_pii', 'partial_pii', 'full_pii']), sorted(names))

        def test_scan_dbexplorer(self):
            self.explorer.scan()
            schema = self.explorer.get_schemas()[0]
            self.assertTrue(schema.has_pii())


char_data_types = """
create table char_columns(ch char, chn char(10), vchn varchar(10));
create table no_char_columns(i int, d float, t time, dt date, ts timestamp);
create table some_char_columns(vchn varchar(10), txt text, i int)
"""


class CommonDataTypeTestCases:
    class CommonDataTypeTests(TestCase, ABC):
        @abstractmethod
        def get_test_schema(self):
            pass

        def test_char_columns(self):
            names = [col.get_name() for col in self.explorer.get_columns(self.get_test_schema(), "char_columns")]
            self.assertEqual(['ch', 'chn', 'vchn'], names)

        def test_some_char_columns(self):
            names = [col.get_name() for col in self.explorer.get_columns(self.get_test_schema(), "some_char_columns")]
            self.assertEqual(['txt', 'vchn'], names)

        def test_tables(self):
            names = [tbl.get_name() for tbl in self.explorer.get_tables(self.get_test_schema())]
            self.assertEqual(sorted(['char_columns', 'some_char_columns']), sorted(names))


@pytest.mark.usefixtures("temp_sqlite")
@pytest.mark.dbtest
class SqliteExplorerTest(CommonExplorerTestCases.CommonExplorerTests):

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
            logging.info("Deleted {}".format(str(self.temp_dir)))

        request.addfinalizer(finalizer)

    def setUp(self):
        self.explorer = SqliteExplorer(str(self.sqlite_conn))

    def tearDown(self):
        self.explorer.get_connection().close()

    def test_schema(self):
        names = [sch.get_name() for sch in self.explorer.get_schemas()]
        self.assertEqual([''], names)

    def get_test_schema(self):
        return ""


@pytest.mark.usefixtures("temp_sqlite")
@pytest.mark.dbtest
class SqliteDataTypeTest(CommonDataTypeTestCases.CommonDataTypeTests):

    @pytest.fixture(scope="class")
    def temp_sqlite(self, request, tmpdir_factory):
        request.cls.temp_dir = tmpdir_factory.mktemp("sqlite_test")
        request.cls.sqlite_conn = request.cls.temp_dir.join("data_type_tests")

        self.conn = sqlite3.connect(str(request.cls.sqlite_conn))
        self.conn.executescript(char_data_types)
        self.conn.commit()
        self.conn.close()

        def finalizer():
            rmtree(self.temp_dir)
            logging.info("Deleted {}".format(str(self.temp_dir)))

        request.addfinalizer(finalizer)

    def setUp(self):
        self.explorer = SqliteExplorer(str(self.sqlite_conn))

    def tearDown(self):
        self.explorer.get_connection().close()

    def get_test_schema(self):
        return ""


@pytest.mark.usefixtures("create_tables")
@pytest.mark.dbtest
class MySQLExplorerTest(CommonExplorerTestCases.CommonExplorerTests):
    pii_db_query = """
        CREATE DATABASE IF NOT EXISTS pii_db;
        use pii_db;
    """

    pii_db_drop = """
        DROP DATABASE IF EXISTS pii_db
    """

    @staticmethod
    def execute_script(cursor, script):
        for query in script.split(';'):
            if len(query.strip()) > 0:
                cursor.execute(query)

    @pytest.fixture(scope="class")
    def create_tables(self, request):
        self.conn = pymysql.connect(host="127.0.0.1",
                                    user="pii_tester",
                                    password="pii_secret")

        with self.conn.cursor() as cursor:
            self.execute_script(cursor, self.pii_db_query)
            self.execute_script(cursor, pii_data_script)
            cursor.execute("commit")
            cursor.close()

        def drop_tables():
            with self.conn.cursor() as cursor:
                cursor.execute(self.pii_db_drop)
                logging.info("Executed drop script")
                cursor.close()
            self.conn.close()

        request.addfinalizer(drop_tables)

    def setUp(self):
        self.explorer = MySQLExplorer(host="127.0.0.1",
                                      user="pii_tester",
                                      password="pii_secret")

    def tearDown(self):
        self.explorer.get_connection().close()

    def test_schema(self):
        names = [sch.get_name() for sch in self.explorer.get_schemas()]
        self.assertEqual(['pii_db'], names)

    def get_test_schema(self):
        return "pii_db"

@pytest.mark.usefixtures("create_tables")
@pytest.mark.dbtest
class MySQLDataTypeTest(CommonDataTypeTestCases.CommonDataTypeTests):
    char_db_query = """
        CREATE DATABASE IF NOT EXISTS pii_db;
        use pii_db;
    """

    char_db_drop = """
        DROP DATABASE IF EXISTS pii_db
    """

    @staticmethod
    def execute_script(cursor, script):
        for query in script.split(';'):
            if len(query.strip()) > 0:
                cursor.execute(query)

    @pytest.fixture(scope="class")
    def create_tables(self, request):
        self.conn = pymysql.connect(host="127.0.0.1",
                                    user="pii_tester",
                                    password="pii_secret")

        with self.conn.cursor() as cursor:
            self.execute_script(cursor, self.char_db_query)
            self.execute_script(cursor, char_data_types)
            cursor.execute("commit")
            cursor.close()

        def drop_tables():
            with self.conn.cursor() as drop_cursor:
                drop_cursor.execute(self.char_db_drop)
                logging.info("Executed drop script")
                drop_cursor.close()
            self.conn.close()

        request.addfinalizer(drop_tables)

    def setUp(self):
        self.explorer = MySQLExplorer(host="127.0.0.1",
                                      user="pii_tester",
                                      password="pii_secret")

    def tearDown(self):
        self.explorer.get_connection().close()

    def get_test_schema(self):
        return "pii_db"


@pytest.mark.usefixtures("create_tables")
@pytest.mark.dbtest
class PostgresDataTypeTest(CommonDataTypeTestCases.CommonDataTypeTests):
    char_db_drop = """
        DROP TABLE char_columns;
        DROP TABLE no_char_columns;
        DROP TABLE some_char_columns;
    """

    @staticmethod
    def execute_script(cursor, script):
        for query in script.split(';'):
            if len(query.strip()) > 0:
                cursor.execute(query)

    @pytest.fixture(scope="class")
    def create_tables(self, request):
        self.conn = psycopg2.connect(host="127.0.0.1",
                                     user="postgres",
                                     password="pii_secret")

        self.conn.autocommit = True

        with self.conn.cursor() as cursor:
            self.execute_script(cursor, char_data_types)
            cursor.close()

        def drop_tables():
            with self.conn.cursor() as d_cursor:
                d_cursor.execute(self.char_db_drop)
                logging.info("Executed drop script")
                d_cursor.close()
            self.conn.close()

        request.addfinalizer(drop_tables)

    def setUp(self):
        self.explorer = PostgreSQLExplorer(host="127.0.0.1",
                                           user="postgres",
                                           password="pii_secret",
                                           database="postgres")

    def tearDown(self):
        self.explorer.get_connection().close()

    def get_test_schema(self):
        return "public"


@pytest.mark.usefixtures("create_tables")
@pytest.mark.dbtest
class PostgresExplorerTest(CommonExplorerTestCases.CommonExplorerTests):
    pii_db_drop = """
        DROP TABLE full_pii;
        DROP TABLE partial_pii;
        DROP TABLE no_pii;
    """

    @staticmethod
    def execute_script(cursor, script):
        for query in script.split(';'):
            if len(query.strip()) > 0:
                cursor.execute(query)

    @pytest.fixture(scope="class")
    def create_tables(self, request):
        self.conn = psycopg2.connect(host="127.0.0.1",
                                     user="postgres",
                                     password="pii_secret")

        self.conn.autocommit = True

        with self.conn.cursor() as cursor:
            self.execute_script(cursor, pii_data_script)
            cursor.close()

        def drop_tables():
            with self.conn.cursor() as d_cursor:
                d_cursor.execute(self.pii_db_drop)
                logging.info("Executed drop script")
                d_cursor.close()
            self.conn.close()

        request.addfinalizer(drop_tables)

    def setUp(self):
        self.explorer = PostgreSQLExplorer(host="127.0.0.1",
                                           user="postgres",
                                           password="pii_secret",
                                           database="postgres")

    def tearDown(self):
        self.explorer.get_connection().close()

    def test_schema(self):
        names = [sch.get_name() for sch in self.explorer.get_schemas()]
        self.assertEqual(['public'], names)

    def get_test_schema(self):
        return "public"

