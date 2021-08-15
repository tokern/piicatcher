from argparse import Namespace

import psycopg2
import pymysql
import pytest
import yaml
from dbcat.catalog.models import PiiTypes

from piicatcher.explorer.databases import RelDbExplorer

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


pii_data_load = [
    "create table no_pii(a text, b text)",
    "insert into no_pii values ('abc', 'def')",
    "insert into no_pii values ('xsfr', 'asawe')",
    "create table partial_pii(a text, b text)",
    "insert into partial_pii values ('917-908-2234', 'plkj')",
    "insert into partial_pii values ('215-099-2234', 'sfrf')",
    "create table full_pii(name text, location text)",
    "insert into full_pii values ('Jonathan Smith', 'Virginia')",
    "insert into full_pii values ('Chase Ryan', 'Chennai')",
]

pii_data_drop = ["DROP TABLE full_pii", "DROP TABLE partial_pii", "DROP TABLE no_pii"]


def mysql_conn():
    return (
        pymysql.connect(
            host="127.0.0.1", user="piiuser", password="p11secret", database="piidb",
        ),
        "piidb",
    )


def pg_conn():
    return (
        psycopg2.connect(
            host="127.0.0.1", user="piiuser", password="p11secret", database="piidb"
        ),
        "public",
    )


@pytest.fixture(scope="module")
def load_all_data():
    params = [mysql_conn(), pg_conn()]
    for p in params:
        db_conn, expected_schema = p
        with db_conn.cursor() as cursor:
            for statement in pii_data_load:
                cursor.execute(statement)
            cursor.execute("commit")
    yield params
    for p in params:
        db_conn, expected_schema = p
        with db_conn.cursor() as cursor:
            for statement in pii_data_drop:
                cursor.execute(statement)
            cursor.execute("commit")

    for p in params:
        db_conn, expected_schema = p
        db_conn.close()


@pytest.fixture(scope="module")
def setup_explorer(open_catalog_connection, load_all_data):
    catalog_obj, conf = open_catalog_connection
    config_yaml = yaml.safe_load(conf)
    config_yaml["catalog"]["format"] = "db"

    namespace = Namespace(
        catalog=config_yaml["catalog"],
        catalog_conf=conf,
        host="127.0.0.1",
        user="piiuser",
        password="p11secret",
        database="piidb",
        connection_type="postgresql",
        scan_type=None,
        include_schema=(),
        exclude_schema=(),
        include_table=(),
        exclude_table=(),
    )

    RelDbExplorer.dispatch(namespace)
    yield catalog_obj


@pytest.fixture
def managed_session(setup_explorer):
    catalog_obj = setup_explorer
    with catalog_obj.managed_session:
        yield catalog_obj


def test_get_source(managed_session):
    catalog_obj = managed_session
    source = catalog_obj.get_source("database")
    assert source.fqdn == "database"


def test_get_schema(managed_session):
    catalog_obj = managed_session
    schema = catalog_obj.get_schema("database", "public")
    assert schema.fqdn == ("database", "public")


def test_get_tables(managed_session):
    catalog_obj = managed_session
    tables = catalog_obj.search_tables(
        source_like="database", schema_like="public", table_like="%"
    )

    assert len(tables) == 3
    assert tables[0].fqdn == ("database", "public", "full_pii")
    assert tables[1].fqdn == ("database", "public", "no_pii")
    assert tables[2].fqdn == ("database", "public", "partial_pii")


def test_column(managed_session):
    catalog_obj = managed_session
    column = catalog_obj.get_column(
        source_name="database",
        schema_name="public",
        table_name="full_pii",
        column_name="name",
    )
    assert column.pii_type == PiiTypes.PERSON
