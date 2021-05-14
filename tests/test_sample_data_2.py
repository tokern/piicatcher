import csv
import gzip
from argparse import Namespace

import pytest

from piicatcher.explorer.databases import PostgreSQLExplorer
from tests.test_databases import pg_conn


@pytest.fixture
def load_sample_data_2():
    connection = pg_conn()[1]
    create_table = """
        CREATE TABLE SAMPLE_2(
            ID VARCHAR(255),CREATED_BY VARCHAR(255),CREATED_ON VARCHAR(255),UPDATED_BY VARCHAR(255),UPDATED_ON VARCHAR(255),DELETED VARCHAR(255),DISABLED VARCHAR(255),ADDEDFROMUI VARCHAR(255),ISCURRENT VARCHAR(255),NAME VARCHAR(255),OPTLOCKVER VARCHAR(255),VALID_FROM VARCHAR(255),VALID_TILL VARCHAR(255),INACTIVE VARCHAR(255),ISADMIN VARCHAR(255),LOCKED VARCHAR(255),PASSWORD VARCHAR(255),USERNAME VARCHAR(255),SUPERVISOR VARCHAR(255),FIRST_NAME VARCHAR(255),IS_MANAGER VARCHAR(255),LAST_NAME VARCHAR(255),MANAGER VARCHAR(255),ASSET_ID VARCHAR(255),"phone numbers" VARCHAR(255),"Social Security number" VARCHAR(255),DOB VARCHAR(255),email VARCHAR(255),address VARCHAR(255),city VARCHAR(255),state VARCHAR(255),zip VARCHAR(255)
        )
    """

    sql = """
        INSERT INTO SAMPLE_2 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,
                                  %s,%s,%s,%s,%s,%s,%s,%s,
                                  %s,%s,%s,%s,%s,%s,%s,%s,
                                  %s,%s,%s,%s,%s,%s,%s,%s
                                  )
    """
    # Get  Data

    with gzip.open("tests/samples/sample-data-2.csv.gz", "rt") as csv_file:
        reader = csv.reader(csv_file)

        with connection.cursor() as cursor:
            cursor.execute(create_table)
            header = True
            for row in reader:
                if not header:
                    cursor.execute(sql, row)
                header = False
            connection.commit()

    yield

    drop_table = "DROP TABLE SAMPLE_2"

    with connection.cursor() as cursor:
        cursor.execute(drop_table)
        connection.commit()


@pytest.fixture
def pg_explorer():
    return PostgreSQLExplorer(
        Namespace(
            host="127.0.0.1",
            user="piiuser",
            password="p11secret",
            database="piidb",
            include_schema=(),
            exclude_schema=(),
            include_table=(),
            exclude_table=(),
            catalog=None,
        )
    )


def test_shallow_scan(load_sample_data_2, pg_explorer):
    try:
        pg_explorer.shallow_scan()
    finally:
        pg_explorer.close_connection()
    assert pg_explorer.get_tabular(True) == [
        ["public", "sample_2", "Social Security number", True],
        ["public", "sample_2", "addedfromui", False],
        ["public", "sample_2", "address", True],
        ["public", "sample_2", "asset_id", False],
        ["public", "sample_2", "city", True],
        ["public", "sample_2", "created_by", False],
        ["public", "sample_2", "created_on", False],
        ["public", "sample_2", "deleted", False],
        ["public", "sample_2", "disabled", False],
        ["public", "sample_2", "dob", True],
        ["public", "sample_2", "email", True],
        ["public", "sample_2", "first_name", True],
        ["public", "sample_2", "id", False],
        ["public", "sample_2", "inactive", False],
        ["public", "sample_2", "is_manager", False],
        ["public", "sample_2", "isadmin", False],
        ["public", "sample_2", "iscurrent", False],
        ["public", "sample_2", "last_name", True],
        ["public", "sample_2", "locked", False],
        ["public", "sample_2", "manager", False],
        ["public", "sample_2", "name", True],
        ["public", "sample_2", "optlockver", False],
        ["public", "sample_2", "password", True],
        ["public", "sample_2", "phone numbers", False],
        ["public", "sample_2", "state", True],
        ["public", "sample_2", "supervisor", False],
        ["public", "sample_2", "updated_by", False],
        ["public", "sample_2", "updated_on", False],
        ["public", "sample_2", "username", True],
        ["public", "sample_2", "valid_from", False],
        ["public", "sample_2", "valid_till", False],
        ["public", "sample_2", "zip", False],
    ]


@pytest.mark.skip
def test_deep_scan(load_sample_data_2, pg_explorer):
    try:
        pg_explorer.scan()
    finally:
        pg_explorer.close_connection()
    assert pg_explorer.get_tabular(True) == [
        ["public", "sample_2", "Social Security number", True],
        ["public", "sample_2", "addedfromui", False],
        ["public", "sample_2", "address", True],
        ["public", "sample_2", "asset_id", False],
        ["public", "sample_2", "city", True],
        ["public", "sample_2", "created_by", False],
        ["public", "sample_2", "created_on", False],
        ["public", "sample_2", "deleted", False],
        ["public", "sample_2", "disabled", False],
        ["public", "sample_2", "dob", True],
        ["public", "sample_2", "email", True],
        ["public", "sample_2", "first_name", True],
        ["public", "sample_2", "id", True],
        ["public", "sample_2", "inactive", False],
        ["public", "sample_2", "is_manager", False],
        ["public", "sample_2", "isadmin", False],
        ["public", "sample_2", "iscurrent", False],
        ["public", "sample_2", "last_name", True],
        ["public", "sample_2", "locked", False],
        ["public", "sample_2", "manager", True],
        ["public", "sample_2", "name", True],
        ["public", "sample_2", "optlockver", False],
        ["public", "sample_2", "password", True],
        ["public", "sample_2", "phone numbers", True],
        ["public", "sample_2", "state", True],
        ["public", "sample_2", "supervisor", True],
        ["public", "sample_2", "updated_by", False],
        ["public", "sample_2", "updated_on", False],
        ["public", "sample_2", "username", True],
        ["public", "sample_2", "valid_from", False],
        ["public", "sample_2", "valid_till", False],
        ["public", "sample_2", "zip", True],
    ]
