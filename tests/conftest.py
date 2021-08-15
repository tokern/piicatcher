from contextlib import closing

import pytest
import yaml
from dbcat import catalog_connection, init_db
from dbcat.catalog.catalog import PGCatalog
from pytest_lazyfixture import lazy_fixture


def pytest_configure(config):
    config.addinivalue_line("markers", "dbtest: Tests that require a database to run")


postgres_conf = """
catalog:
  user: piiuser
  password: p11secret
  host: 127.0.0.1
  port: 5432
  database: piidb
"""


@pytest.fixture(scope="session")
def root_connection():
    config = yaml.safe_load(postgres_conf)
    with closing(PGCatalog(**config["catalog"])) as conn:
        yield conn


sqlite_catalog_conf = """
catalog:
  path: {path}
"""


@pytest.fixture(scope="session")
def setup_sqlite(tmpdir_factory):
    temp_dir = tmpdir_factory.mktemp("sqlite_test")
    sqlite_path = temp_dir.join("sqldb")

    yield None, sqlite_catalog_conf.format(path=sqlite_path)


pg_catalog_conf = """
catalog:
  user: catalog_user
  password: catal0g_passw0rd
  host: 127.0.0.1
  port: 5432
  database: tokern
"""


@pytest.fixture(scope="session")
def setup_pg(root_connection):
    with root_connection.engine.connect() as conn:
        conn.execute("CREATE USER catalog_user PASSWORD 'catal0g_passw0rd'")
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(
            "CREATE DATABASE tokern"
        )
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(
            "GRANT ALL PRIVILEGES ON DATABASE tokern TO catalog_user"
        )

    yield root_connection, pg_catalog_conf

    with root_connection.engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(
            "DROP DATABASE tokern"
        )

        conn.execution_options(isolation_level="AUTOCOMMIT").execute(
            "DROP USER catalog_user"
        )


@pytest.fixture(
    scope="session", params=[lazy_fixture("setup_sqlite"), lazy_fixture("setup_pg")]
)
def setup_catalog(request):
    yield request.param


@pytest.fixture(scope="session")
def open_catalog_connection(setup_catalog):
    connection, conf = setup_catalog
    with closing(catalog_connection(conf)) as conn:
        init_db(conn)
        yield conn, conf
