from abc import ABC, abstractmethod
from unittest import TestCase, mock

from argparse import Namespace
import pymysql
import psycopg2
import logging
import pytest

from piicatcher.explorer.databases import MySQLExplorer, PostgreSQLExplorer, OracleExplorer, \
    MSSQLExplorer, RelDbExplorer
from piicatcher.explorer.sqlite import SqliteExplorer
from piicatcher.explorer.metadata import Schema, Table, Column
from piicatcher.piitypes import PiiTypes

logging.basicConfig(level=logging.DEBUG)


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
            for schema in self.explorer.get_schemas():
                if schema.get_name() == self.get_test_schema():
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


@pytest.mark.usefixtures("create_tables")
class MySQLExplorerTest(CommonExplorerTestCases.CommonExplorerTests):

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
        self.conn = pymysql.connect(host="127.0.0.1",
                                    user="piiuser",
                                    password="p11secret",
                                    database="piidb",
                                    )

        with self.conn.cursor() as cursor:
            self.execute_script(cursor, pii_data_script)
            cursor.execute("commit")
            cursor.close()

        def drop_tables():
            with self.conn.cursor() as cursor:
                self.execute_script(cursor, self.pii_db_drop)
                logging.info("Executed drop script")
                cursor.close()
            self.conn.close()

        request.addfinalizer(drop_tables)

    def setUp(self):
        self.explorer = MySQLExplorer(Namespace(
            host="127.0.0.1",
            user="piiuser",
            password="p11secret",
            database="piidb",
            include_schema=(),
            exclude_schema=(),
            include_table=(),
            exclude_table=(),
            catalog=None
        ))

    def tearDown(self):
        self.explorer.get_connection().close()

    def test_schema(self):
        names = [sch.get_name() for sch in self.explorer.get_schemas()]
        self.assertEqual(['piidb'], names)
        return "piidb"

    def get_test_schema(self):
        return "piidb"


@pytest.mark.usefixtures("create_tables")
class MySQLDataTypeTest(CommonDataTypeTestCases.CommonDataTypeTests):
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
        self.conn = pymysql.connect(host="127.0.0.1",
            user="piiuser",
            password="p11secret",
            database="piidb"
        )

        with self.conn.cursor() as cursor:
            self.execute_script(cursor, char_data_types)
            cursor.execute("commit")
            cursor.close()

        def drop_tables():
            with self.conn.cursor() as drop_cursor:
                self.execute_script(drop_cursor, self.char_db_drop)
                logging.info("Executed drop script")
                drop_cursor.close()
            self.conn.close()

        request.addfinalizer(drop_tables)

    def setUp(self):
        self.explorer = MySQLExplorer(Namespace(
            host="127.0.0.1",
            user="piiuser",
            password="p11secret",
            database="piidb",
            include_schema=(),
            exclude_schema=(),
            include_table=(),
            exclude_table=(),
            catalog=None
        ))

    def tearDown(self):
        self.explorer.get_connection().close()

    def get_test_schema(self):
        return "piidb"


@pytest.mark.usefixtures("create_tables")
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
                                     user="piiuser",
                                     password="p11secret",
                                     database="piidb")

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
        self.explorer = PostgreSQLExplorer(Namespace(
            host="127.0.0.1",
            user="piiuser",
            password="p11secret",
            database="piidb",
            include_schema=(),
            exclude_schema=(),
            include_table=(),
            exclude_table=(),
            catalog=None
        ))

    def tearDown(self):
        self.explorer.get_connection().close()

    def get_test_schema(self):
        return "public"


@pytest.mark.usefixtures("create_tables")
class PostgresExplorerTest(CommonExplorerTestCases.CommonExplorerTests):
    pii_db_drop = """
        DROP TABLE full_pii;
        DROP TABLE partial_pii;
        DROP TABLE no_pii;
        DROP SCHEMA company cascade;
    """

    second_schema = """
        CREATE SCHEMA company;
        CREATE TABLE company.employees(name varchar, designation varchar);
        CREATE TABLE company.departments(name varchar, manager varchar);
    """
    @staticmethod
    def execute_script(cursor, script):
        for query in script.split(';'):
            if len(query.strip()) > 0:
                cursor.execute(query)

    @pytest.fixture(scope="class")
    def create_tables(self, request):
        self.conn = psycopg2.connect(host="127.0.0.1",
                                     user="piiuser",
                                     password="p11secret",
                                     database="piidb")

        self.conn.autocommit = True

        with self.conn.cursor() as cursor:
            self.execute_script(cursor, pii_data_script)
            self.execute_script(cursor, self.second_schema)
            cursor.close()

        def drop_tables():
            with self.conn.cursor() as d_cursor:
                d_cursor.execute(self.pii_db_drop)
                logging.info("Executed drop script")
                d_cursor.close()
            self.conn.close()

        request.addfinalizer(drop_tables)

    def setUp(self):
        self.explorer = PostgreSQLExplorer(Namespace(
            host="127.0.0.1",
            user="piiuser",
            password="p11secret",
            database="piidb",
            include_schema=(),
            exclude_schema=(),
            include_table=(),
            exclude_table=(),
            catalog=None
        ))

    def tearDown(self):
        self.explorer.get_connection().close()

    def test_schema(self):
        names = [sch.get_name() for sch in self.explorer.get_schemas()]
        self.assertCountEqual(['public', 'company'], names)

    def get_test_schema(self):
        return "public"


class SelectQueryTest:
    def setUp(self):
        col1 = Column('c1')
        col2 = Column('c2')
        col2._pii = [PiiTypes.LOCATION]

        self.schema = Schema('testSchema')

        table = Table(self.schema, 't1')
        table._columns = [col1, col2]

        self.schema.tables = [table]

    def test_oracle(self):
        self.assertEqual("select c1, c2 from t1 sample(5)",
                         OracleExplorer._get_select_query(self.schema,
                                                          self.schema.get_tables()[0],
                                                          self.schema.get_tables()[0].get_columns()))

    def test_sqlite(self):
        self.assertEqual("select c1, c2 from t1",
                         SqliteExplorer._get_select_query(self.schema,
                                                          self.schema.get_tables()[0],
                                                          self.schema.get_tables()[0].get_columns()))

    def test_postgres(self):
        self.assertEqual("select c1, c2 from testSchema.t1",
                         PostgreSQLExplorer._get_select_query(self.schema,
                                                              self.schema.get_tables()[0],
                                                              self.schema.get_tables()[0].get_columns()))

    def test_mysql(self):
        self.assertEqual("select c1, c2 from testSchema.t1",
                         MySQLExplorer._get_select_query(self.schema,
                                                         self.schema.get_tables()[0],
                                                         self.schema.get_tables()[0].get_columns()))

    def test_mssql(self):
        self.assertEqual("select c1, c2 from testSchema.t1",
                         MSSQLExplorer._get_select_query(self.schema,
                                                         self.schema.get_tables()[0],
                                                         self.schema.get_tables()[0].get_columns()))


class TestDispatcher(TestCase):

    def test_mysql_dispatch(self):
        with mock.patch('piicatcher.explorer.databases.MySQLExplorer.scan', autospec=True) as mock_scan_method:
            with mock.patch('piicatcher.explorer.databases.MySQLExplorer.get_tabular', autospec=True) as mock_tabular_method:
                with mock.patch('piicatcher.explorer.explorer.tableprint', autospec=True) as MockTablePrint:
                    RelDbExplorer.dispatch(Namespace(host='connection',
                                                     port=None,
                                                     list_all=None,
                                                     output_format='ascii_table',
                                                     connection_type='mysql',
                                                     scan_type='deep',
                                                     catalog=None,
                                                     user='user',
                                                     include_schema=(),
                                                     exclude_schema=(),
                                                     include_table=(),
                                                     exclude_table=(),
                                                     password='pass'))
                    mock_scan_method.assert_called_once()
                    mock_tabular_method.assert_called_once()
                    MockTablePrint.table.assert_called_once()

    def test_postgres_dispatch(self):
        with mock.patch('piicatcher.explorer.databases.PostgreSQLExplorer.scan', autospec=True) as mock_scan_method:
            with mock.patch('piicatcher.explorer.databases.PostgreSQLExplorer.get_tabular', autospec=True) as mock_tabular_method:
                with mock.patch('piicatcher.explorer.explorer.tableprint', autospec=True) as MockTablePrint:
                    RelDbExplorer.dispatch(Namespace(host='connection',
                                                     port=None,
                                                     list_all=None,
                                                     output_format='ascii_table',
                                                     connection_type='postgres',
                                                     database='public',
                                                     scan_type=None,
                                                     catalog=None,
                                                     include_schema=(),
                                                     exclude_schema=(),
                                                     include_table=(),
                                                     exclude_table=(),
                                                     user='user',
                                                     password='pass'))
                    mock_scan_method.assert_called_once()
                    mock_tabular_method.assert_called_once()
                    MockTablePrint.table.assert_called_once()

    def test_mysql_shallow_scan(self):
        with mock.patch('piicatcher.explorer.databases.MySQLExplorer.shallow_scan', autospec=True) as mock_shallow_scan_method:
            with mock.patch('piicatcher.explorer.databases.MySQLExplorer.get_tabular', autospec=True) as mock_tabular_method:
                with mock.patch('piicatcher.explorer.explorer.tableprint', autospec=True) as MockTablePrint:
                    RelDbExplorer.dispatch(Namespace(host='connection',
                                                     port=None,
                                                     list_all=None,
                                                     output_format='ascii_table',
                                                     connection_type='mysql',
                                                     catalog=None,
                                                     include_schema=(),
                                                     exclude_schema=(),
                                                     include_table=(),
                                                     exclude_table=(),
                                                     user='user',
                                                     password='pass',
                                                     scan_type="shallow"))
                    mock_shallow_scan_method.assert_called_once()
                    mock_tabular_method.assert_called_once()
                    MockTablePrint.table.assert_called_once()

