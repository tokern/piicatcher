import csv
import logging
from contextlib import closing
from pathlib import Path
from shutil import rmtree
from typing import Any, Generator, Tuple

import dbcat.settings
import pytest
from dbcat.api import catalog_connection_yaml, init_db, scan_sources
from dbcat.catalog.catalog import Catalog
from pytest_cases import fixture, parametrize_with_cases
from sqlalchemy import create_engine
from sqlalchemy.orm.exc import NoResultFound

postgres_conf = """
catalog:
  user: piiuser
  password: p11secret
  host: {host}
  port: 5432
  database: piidb
  secret: {secret}
"""


sqlite_catalog_conf = """
catalog:
  path: {path}
  secret: {secret}
"""


def pytest_addoption(parser):
    parser.addoption(
        "--pg-host", action="store", default="127.0.0.1", help="specify IP of pg host"
    )
    parser.addoption(
        "--mysql-host",
        action="store",
        default="127.0.0.1",
        help="specify IP of mysql host",
    )


@fixture(scope="module")
def app_dir_path(tmp_path_factory) -> Generator[Path, None, None]:
    app_dir = tmp_path_factory.mktemp("piicatcher_app_dir")
    yield app_dir


@fixture(scope="module")
def temp_sqlite_path(app_dir_path) -> Generator[Path, None, None]:
    sqlite_path = app_dir_path / "sqldb"

    yield sqlite_path


def case_setup_sqlite(temp_sqlite_path):
    return sqlite_catalog_conf.format(
        path=temp_sqlite_path, secret=dbcat.settings.DEFAULT_CATALOG_SECRET
    )


pg_catalog_conf = """
catalog:
  user: catalog_user
  password: catal0g_passw0rd
  host: {host}
  port: 5432
  database: tokern
  secret: {secret}
"""


@fixture(scope="module")
def setup_pg_catalog(request):
    conf = postgres_conf.format(
        host=request.config.getoption("--pg-host"),
        secret=dbcat.settings.DEFAULT_CATALOG_SECRET,
    )
    with closing(catalog_connection_yaml(conf)) as root_connection:
        with root_connection.engine.connect() as conn:
            conn.execute("CREATE USER catalog_user PASSWORD 'catal0g_passw0rd'")
            conn.execution_options(isolation_level="AUTOCOMMIT").execute(
                "CREATE DATABASE tokern"
            )
            conn.execution_options(isolation_level="AUTOCOMMIT").execute(
                "GRANT ALL PRIVILEGES ON DATABASE tokern TO catalog_user"
            )

        yield pg_catalog_conf.format(
            host=request.config.getoption("--pg-host"),
            secret=dbcat.settings.DEFAULT_CATALOG_SECRET,
        )

        with root_connection.engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT").execute(
                "DROP DATABASE tokern"
            )

            conn.execution_options(isolation_level="AUTOCOMMIT").execute(
                "DROP USER catalog_user"
            )


def case_setup_pg(setup_pg_catalog):
    return setup_pg_catalog


@fixture(scope="module")
@parametrize_with_cases("catalog_conf", cases=".", scope="module")
def open_catalog_connection(catalog_conf) -> Generator[Catalog, None, None]:
    with closing(catalog_connection_yaml(catalog_conf)) as conn:
        init_db(conn)
        yield conn


pii_data_load = [
    "create table no_pii(a text, b text)",
    "insert into no_pii values ('abc', 'def')",
    "insert into no_pii values ('xsfr', 'asawe')",
    "create table partial_pii(a text, b text)",
    "insert into partial_pii values ('917-908-2234', 'plkj')",
    "insert into partial_pii values ('215-099-2234', 'sfrf')",
    "create table full_pii(name text, state text)",
    "insert into full_pii values ('Jonathan Smith', 'Virginia')",
    "insert into full_pii values ('Chase Ryan', 'Chennai')",
    "create table partial_data_type(id int, ssn text)",
    "insert into partial_data_type values (1, '000-000-1111')",
    "insert into partial_data_type values (2, '111-222-3333')",
]

pii_data_drop = [
    "DROP TABLE full_pii",
    "DROP TABLE partial_pii",
    "DROP TABLE no_pii",
    "DROP TABLE partial_data_type",
]


def source_mysql(open_catalog_connection, request) -> Tuple[Catalog, int]:
    catalog = open_catalog_connection
    host = request.config.getoption("--mysql-host")
    with catalog.managed_session:
        try:
            source = catalog.get_source("mysql_src")
        except NoResultFound:
            source = catalog.add_source(
                name="mysql_src",
                source_type="mysql",
                uri=host,
                username="piiuser",
                password="p11secret",
                database="piidb",
            )
        source_id = source.id

    return catalog, source_id


def source_pg(open_catalog_connection, request) -> Tuple[Catalog, int]:
    catalog = open_catalog_connection
    host = request.config.getoption("--pg-host")
    with catalog.managed_session:
        try:
            source = catalog.get_source("pg_src")
        except NoResultFound:
            source = catalog.add_source(
                name="pg_src",
                source_type="postgresql",
                uri=host,
                username="piiuser",
                password="p11secret",
                database="piidb",
                cluster="public",
            )
        source_id = source.id

    return catalog, source_id


@pytest.fixture(scope="module")
def temp_sqlite(tmp_path_factory):
    temp_dir = tmp_path_factory.mktemp("sqlite_test")
    sqlite_path = temp_dir / "sqldb"

    yield sqlite_path

    rmtree(temp_dir)
    logging.info("Deleted {}", str(temp_dir))


def source_sqlite(
    open_catalog_connection, temp_sqlite
) -> Generator[Tuple[Catalog, int], None, None]:
    catalog = open_catalog_connection
    sqlite_path = temp_sqlite
    with catalog.managed_session:
        try:
            source = catalog.get_source("sqlite_src")
        except NoResultFound:
            source = catalog.add_source(
                name="sqlite_src", source_type="sqlite", uri=str(sqlite_path)
            )
        yield catalog, source.id


@fixture(scope="module")
@parametrize_with_cases("source", scope="module", cases=".", prefix="source_")
def create_source_engine(
    source,
) -> Generator[Tuple[Catalog, int, Any, str, str], None, None]:
    catalog, source_id = source
    with catalog.managed_session:
        source = catalog.get_source_by_id(source_id)
        name = source.name
        conn_string = source.conn_string
        source_type = source.source_type

    engine = create_engine(conn_string)
    yield catalog, source_id, engine, name, source_type
    engine.dispose()


@pytest.fixture(scope="module")
def load_data(create_source_engine) -> Generator[Tuple[Catalog, int, str], None, None]:
    catalog, source_id, engine, name, source_type = create_source_engine
    with engine.begin() as conn:
        for statement in pii_data_load:
            conn.execute(statement)
        if source_type != "sqlite":
            conn.execute("commit")

    yield catalog, source_id, name

    with engine.begin() as conn:
        for statement in pii_data_drop:
            conn.execute(statement)
        if source_type != "sqlite":
            conn.execute("commit")


@pytest.fixture(scope="module")
def load_data_and_pull(load_data) -> Generator[Tuple[Catalog, int], None, None]:
    catalog, source_id, name = load_data
    scan_sources(catalog, [name])
    yield catalog, source_id


@fixture(scope="module")
def load_sample_data(
    create_source_engine,
) -> Generator[Tuple[Catalog, int, str], None, None]:
    catalog, source_id, engine, name, source_type = create_source_engine
    with engine.begin() as conn:
        create_table = """
            CREATE TABLE sample(
                id VARCHAR(255), gender VARCHAR(255), birthdate DATE, maiden_name VARCHAR(255), lname VARCHAR(255),
                fname VARCHAR(255), address VARCHAR(255), city VARCHAR(255), state VARCHAR(255), zip VARCHAR(255),
                phone VARCHAR(255), email VARCHAR(255), cc_type VARCHAR(255), cc_number VARCHAR(255), cc_cvc VARCHAR(255),
                cc_expiredate DATE
            )
        """

        sql = """
            INSERT INTO sample VALUES(%s,%s,%s,%s,%s,%s,%s,%s,
                                      %s,%s,%s,%s,%s,%s,%s,%s)
        """

        if source_type == "sqlite":
            sql = """
                INSERT INTO sample VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """

        # Get  Data
        with open("tests/samples/sample-data.csv") as csv_file:
            reader = csv.reader(csv_file)

            conn.execute(create_table)
            header = True
            for row in reader:
                if not header:
                    conn.execute(sql, row)
                header = False
        if source_type != "sqlite":
            conn.execute("commit")

    yield catalog, source_id, name

    with engine.begin() as conn:
        conn.execute("DROP TABLE sample")
        if source_type != "sqlite":
            conn.execute("commit")


@fixture(scope="module")
def load_sample_data_and_pull(
    load_sample_data,
) -> Generator[Tuple[Catalog, int], None, None]:
    catalog, source_id, name = load_sample_data
    scan_sources(catalog, [name])
    yield catalog, source_id
