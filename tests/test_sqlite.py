import logging
import sqlite3
from argparse import Namespace
from shutil import rmtree
from unittest import TestCase, mock

import pytest

from piicatcher.explorer.sqlite import SqliteExplorer
from tests.test_databases import CommonExplorerTestCases, CommonDataTypeTestCases, \
    pii_data_script, char_data_types


@pytest.fixture(scope="class")
def temp_sqlite(request, tmpdir_factory):
    temp_dir = tmpdir_factory.mktemp("sqlite_test")
    sqlite_path = temp_dir.join("sqldb")

    explorer = SqliteExplorer(Namespace(
        path=sqlite_path, catalog=None,
        include_schema=(),
        exclude_schema=(),
        include_table=(),
        exclude_table=()
    ))

    request.cls.explorer = explorer
    request.cls.path = str(sqlite_path)

    def finalizer():
        explorer.get_connection().close()
        rmtree(temp_dir)
        logging.info("Deleted {}", str(temp_dir))

    request.addfinalizer(finalizer)


@pytest.fixture(scope="class")
# pylint: disable=redefined-outer-name, unused-argument
def load_pii(request, temp_sqlite):
    conn = sqlite3.connect(request.cls.path)
    conn.executescript(pii_data_script)
    conn.commit()
    conn.close()


@pytest.mark.usefixtures("load_pii")
class SqliteExplorerTest(CommonExplorerTestCases.CommonExplorerTests):
    def test_schema(self):
        # pylint: disable=no-member
        names = [sch.get_name() for sch in self.explorer.get_schemas()]
        self.assertEqual([''], names)

    def get_test_schema(self):
        return ""


@pytest.fixture(scope="class")
# pylint: disable=redefined-outer-name, unused-argument
def load_char_data(request, temp_sqlite):
    conn = sqlite3.connect(request.cls.path)
    conn.executescript(char_data_types)
    conn.commit()
    conn.close()


@pytest.mark.usefixtures("load_char_data")
class SqliteDataTypeTest(CommonDataTypeTestCases.CommonDataTypeTests):
    def get_test_schema(self):
        return ""


class TestDispatcher(TestCase):
    def test_sqlite_dispatch(self):
        with mock.patch('piicatcher.explorer.sqlite.SqliteExplorer.scan', autospec=True) \
                as mock_scan_method:
            with mock.patch('piicatcher.explorer.sqlite.SqliteExplorer.get_tabular',
                            autospec=True) as mock_tabular_method:
                with mock.patch('piicatcher.explorer.explorer.tableprint', autospec=True) \
                        as mock_table_print:
                    SqliteExplorer.dispatch(Namespace(path='connection', list_all=None,
                                                      catalog={
                                                          'format': 'ascii_table'
                                                      },
                                                      scan_type=None,
                                                      include_schema=(),
                                                      exclude_schema=(),
                                                      include_table=(),
                                                      exclude_table=()
                                                      ))
                    mock_scan_method.assert_called_once()
                    mock_tabular_method.assert_called_once()
                    mock_table_print.table.assert_called_once()
