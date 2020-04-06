from argparse import Namespace
import logging

import pymysql
import pytest

from piicatcher.catalog.db import *
from piicatcher.explorer.explorer import Explorer
from piicatcher.explorer.metadata import Schema, Table, Column, Database as mdDatabase
from piicatcher.piitypes import PiiTypes

logging.basicConfig(level=logging.DEBUG)


catalog = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'catalog_user',
    'password': 'catal0g_passw0rd',
    'format': 'db'
}


@pytest.fixture(scope="module")
def setup_catalog():
    with pymysql.connect(host=catalog['host'],
                         port=catalog['port'],
                         user='root',
                         password='r00tPa33w0rd',
                         database='piidb').cursor() as c:
        c.execute("CREATE USER catalog_user IDENTIFIED BY 'catal0g_passw0rd'")
        c.execute("CREATE DATABASE tokern")
        c.execute("GRANT ALL ON tokern.* TO 'catalog_user'@'%'")


class MockExplorer(Explorer):
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
        schema = Schema("test_store", include=self._include_table, exclude=self._exclude_table)
        schema.add_child(MockExplorer.get_no_pii_table())
        schema.add_child(MockExplorer.get_partial_pii_table())
        schema.add_child(MockExplorer.get_full_pii_table())

        self._database = mdDatabase('database', include=self._include_schema, exclude=self._exclude_schema)
        self._database.add_child(schema)


@pytest.fixture(scope="module")
def setup_explorer(request, setup_catalog):
    ns = Namespace(catalog=catalog,
                   include_schema=(),
                   exclude_schema=(),
                   include_table=(),
                   exclude_table=()
                   )
    explorer = MockExplorer(ns)
    MockExplorer.output(ns, explorer)

    def finalizer():
        model_db_close()

    request.addfinalizer(finalizer)


def get_cursor():
    return pymysql.connect(host=catalog['host'],
                           port=catalog['port'],
                           user=catalog['user'],
                           password=catalog['password'],
                           database='tokern').cursor()


def test_schema(setup_explorer):
    with get_cursor() as c:
        c.execute('select * from dbschemas')
        assert([(1, 'test_store')] == list(c.fetchall()))


def test_tables(setup_explorer):
    with get_cursor() as c:
        c.execute('select * from dbtables order by id')
        assert([(1, 'no_pii', 1), (2, 'partial_pii', 1), (3, 'full_pii', 1)] == list(c.fetchall()))


def test_columns(setup_explorer):
    with get_cursor() as c:
        c.execute('select * from dbcolumns where table_id in (1,2) order by id')
        assert(
            [(1, 'a', '[]', 1),
             (2, 'b', '[]', 1),
             (3, 'a', '[{"__enum__": "PiiTypes.PHONE"}]', 2),
             (4, 'b', '[]', 2)] == list(c.fetchall()))

