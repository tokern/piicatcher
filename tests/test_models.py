import logging
from argparse import Namespace

import pymysql
import pytest

from piicatcher.catalog.db import DbStore, model_db_close
from piicatcher.explorer.explorer import Explorer
from piicatcher.explorer.metadata import Column
from piicatcher.explorer.metadata import Database as mdDatabase
from piicatcher.explorer.metadata import Schema, Table
from piicatcher.piitypes import PiiTypes

logging.basicConfig(level=logging.DEBUG)


catalog = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "catalog_user",
    "password": "catal0g_passw0rd",
    "format": "db",
}


@pytest.fixture(scope="module")
def setup_catalog():
    with pymysql.connect(
        host=catalog["host"],
        port=catalog["port"],
        user="root",
        password="r00tPa33w0rd",
        database="piidb",
    ).cursor() as c:
        c.execute(
            "CREATE USER IF NOT EXISTS catalog_user IDENTIFIED BY 'catal0g_passw0rd'"
        )
        c.execute("CREATE DATABASE IF NOT EXISTS tokern")
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


@pytest.fixture(scope="module")
def setup_explorer(request, setup_catalog):
    ns = Namespace(
        catalog=catalog,
        include_schema=(),
        exclude_schema=(),
        include_table=(),
        exclude_table=(),
    )
    explorer = MockExplorer(ns)
    MockExplorer.output(ns, explorer)

    def finalizer():
        model_db_close()

    request.addfinalizer(finalizer)


def get_connection():
    return pymysql.connect(
        host=catalog["host"],
        port=catalog["port"],
        user=catalog["user"],
        password=catalog["password"],
        database="tokern",
    )


def test_schema(setup_explorer):
    with get_connection().cursor() as c:
        c.execute("select name from dbschemas")
        assert [("test_store",)] == list(c.fetchall())


def test_tables(setup_explorer):
    with get_connection().cursor() as c:
        c.execute("select name from dbtables order by id")
        assert [("no_pii",), ("partial_pii",), ("full_pii",)] == list(c.fetchall())


def test_columns(setup_explorer):
    with get_connection().cursor() as c:
        c.execute("select name, pii_type from dbcolumns order by id")
        assert [
            ("a", "[]"),
            ("b", "[]"),
            ("a", '[{"__enum__": "PiiTypes.PHONE"}]'),
            ("b", "[]"),
            ("a", '[{"__enum__": "PiiTypes.PHONE"}]'),
            (
                "b",
                '[{"__enum__": "PiiTypes.ADDRESS"}, {"__enum__": "PiiTypes.LOCATION"}]',
            ),
        ] == list(c.fetchall())


def test_setup_database(setup_explorer):
    DbStore.setup_database(catalog=catalog)
    with get_connection().cursor() as c:
        c.execute("select name from dbtables order by id")
        assert [("no_pii",), ("partial_pii",), ("full_pii",)] == list(c.fetchall())


def test_out_of_band_update(setup_explorer):
    connection = get_connection()
    with connection.cursor() as c:
        c.execute("select id from dbtables where name = 'partial_pii'")
        table_id = c.fetchone()[0]
        c.execute(
            """
            update dbcolumns set pii_type='[{{"__enum__": "PiiTypes.CREDIT_CARD"}}]'
            where table_id = {0} and name = 'b'
            """.format(
                table_id
            )
        )

    connection.commit()

    with connection.cursor() as c:
        c.execute(
            "select name, pii_type from dbcolumns where table_id = {0} order by id".format(
                table_id
            )
        )
        assert [
            ("a", '[{"__enum__": "PiiTypes.PHONE"}]'),
            ("b", '[{"__enum__": "PiiTypes.CREDIT_CARD"}]'),
        ] == list(c.fetchall())


class UpdateColumnExplorer(MockExplorer):
    @staticmethod
    def get_partial_pii_table():
        partial_pii_table = Table("test_store", "partial_pii")
        partial_pii_a = Column("a")
        partial_pii_a.add_pii_type(PiiTypes.PHONE)
        partial_pii_b = Column("b")
        partial_pii_b.add_pii_type(PiiTypes.ADDRESS)

        partial_pii_table.add_child(partial_pii_a)
        partial_pii_table.add_child(partial_pii_b)

        return partial_pii_table


def test_update(setup_explorer):
    ns = Namespace(
        catalog=catalog,
        include_schema=(),
        exclude_schema=(),
        include_table=(),
        exclude_table=(),
    )
    explorer = UpdateColumnExplorer(ns)
    UpdateColumnExplorer.output(ns, explorer)

    connection = get_connection()
    with connection.cursor() as c:
        c.execute("select id from dbtables where name = 'partial_pii'")
        table_id = c.fetchone()[0]

    with connection.cursor() as c:
        c.execute(
            "select name, pii_type from dbcolumns where table_id = {0} order by id".format(
                table_id
            )
        )
        assert [
            ("a", '[{"__enum__": "PiiTypes.PHONE"}]'),
            ("b", '[{"__enum__": "PiiTypes.CREDIT_CARD"}]'),
        ] == list(c.fetchall())
