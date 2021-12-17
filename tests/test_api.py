from pathlib import Path

import pytest
from dbcat.catalog import Catalog

import piicatcher
from piicatcher.api import (
    ScanTypeEnum,
    list_detectors,
    scan_athena,
    scan_database,
    scan_mysql,
    scan_postgresql,
    scan_redshift,
    scan_snowflake,
    scan_sqlite,
)


def test_detector_list():
    assert list(list_detectors()) == ["ColumnNameRegexDetector", "DatumRegexDetector"]


def test_scan_database_shallow(load_sample_data_and_pull):
    catalog, source_id = load_sample_data_and_pull
    with catalog.managed_session:
        source = catalog.get_source_by_id(source_id)
        scan_database(catalog=catalog, source=source, include_table_regex=["sample"])

        schemata = catalog.search_schema(source_like=source.name, schema_like="%")

        for column_name, pii_type in [
            ("address", piicatcher.Address()),
            ("city", piicatcher.Address()),
            ("email", piicatcher.Email()),
            ("fname", piicatcher.Person()),
            ("gender", piicatcher.Gender()),
            ("lname", piicatcher.Person()),
            ("maiden_name", piicatcher.Person()),
            ("state", piicatcher.Address()),
        ]:
            column = catalog.get_column(
                source_name=source.name,
                schema_name=schemata[0].name,
                table_name="sample",
                column_name=column_name,
            )
            assert column.pii_type == pii_type

        latest_task = catalog.get_latest_task("piicatcher.{}".format(source.name))
        assert latest_task.status == 0
        assert latest_task.created_at is not None
        assert latest_task.updated_at is not None


@pytest.mark.skip
def test_scan_database_deep(load_sample_data_and_pull):
    catalog, source_id = load_sample_data_and_pull
    with catalog.managed_session:
        source = catalog.get_source_by_id(source_id)
        scan_database(
            catalog=catalog,
            source=source,
            include_table_regex=["sample"],
            scan_type=ScanTypeEnum.deep,
        )

        schemata = catalog.search_schema(source_like=source.name, schema_like="%")

        for column_name, pii_type in [("id", piicatcher.BirthDate)]:
            column = catalog.get_column(
                source_name=source.name,
                schema_name=schemata[0].name,
                table_name="sample",
                column_name=column_name,
            )
            assert column.pii_type == pii_type


def test_scan_sqlite(mocker, temp_sqlite_path, app_dir_path):
    mocker.patch("piicatcher.api.scan_database")
    mocker.patch.object(Catalog, "add_source")

    scan_sqlite(
        catalog_params={"path": str(temp_sqlite_path), "app_dir": app_dir_path},
        name="test_scan_sqlite",
        path=Path("/tmp/sqldb"),
        scan_type=ScanTypeEnum.deep,
    )
    piicatcher.api.scan_database.assert_called_once()
    Catalog.add_source.assert_called_once()


def test_scan_pg(mocker, temp_sqlite_path, app_dir_path):
    mocker.patch("piicatcher.api.scan_database")
    mocker.patch.object(Catalog, "add_source")

    scan_postgresql(
        catalog_params={"path": str(temp_sqlite_path), "app_dir": app_dir_path},
        name="test_scan_pg",
        uri="127.0.0.1",
        username="u",
        password="p",
        database="d",
    )
    piicatcher.api.scan_database.assert_called_once()
    Catalog.add_source.assert_called_once()


def test_scan_mysql(mocker, temp_sqlite_path, app_dir_path):
    mocker.patch("piicatcher.api.scan_database")
    mocker.patch.object(Catalog, "add_source")

    scan_mysql(
        catalog_params={"path": str(temp_sqlite_path), "app_dir": app_dir_path},
        name="test_scan_mysql",
        uri="127.0.0.1",
        username="u",
        password="p",
        database="d",
    )
    piicatcher.api.scan_database.assert_called_once()
    Catalog.add_source.assert_called_once()


def test_scan_redshift(mocker, temp_sqlite_path, app_dir_path):
    mocker.patch("piicatcher.api.scan_database")
    mocker.patch.object(Catalog, "add_source")

    scan_redshift(
        catalog_params={"path": str(temp_sqlite_path), "app_dir": app_dir_path},
        name="test_scan_redshift",
        uri="127.0.0.1",
        username="u",
        password="p",
        database="d",
    )
    piicatcher.api.scan_database.assert_called_once()
    Catalog.add_source.assert_called_once()


def test_scan_snowflake(mocker, temp_sqlite_path, app_dir_path):
    mocker.patch("piicatcher.api.scan_database")
    mocker.patch.object(Catalog, "add_source")

    scan_snowflake(
        catalog_params={"path": str(temp_sqlite_path), "app_dir": app_dir_path},
        name="test_scan_redshift",
        account="127.0.0.1",
        username="u",
        password="p",
        database="d",
        warehouse="w",
        role="r",
    )
    piicatcher.api.scan_database.assert_called_once()
    Catalog.add_source.assert_called_once()


def test_scan_athena(mocker, temp_sqlite_path, app_dir_path):
    mocker.patch("piicatcher.api.scan_database")
    mocker.patch.object(Catalog, "add_source")

    scan_athena(
        catalog_params={"path": temp_sqlite_path, "app_dir": app_dir_path},
        name="test_scan_athena",
        aws_access_key_id="a",
        aws_secret_access_key="s",
        region_name="r",
        s3_staging_dir="s3",
    )
    piicatcher.api.scan_database.assert_called_once()
    Catalog.add_source.assert_called_once()


def test_scan_athena_iam(mocker, temp_sqlite_path, app_dir_path):
    mocker.patch("piicatcher.api.scan_database")
    mocker.patch.object(Catalog, "add_source")

    scan_athena(
        catalog_params={"path": temp_sqlite_path, "app_dir": app_dir_path},
        name="test_scan_athena",
        region_name="r",
        s3_staging_dir="s3",
    )
    piicatcher.api.scan_database.assert_called_once()
    Catalog.add_source.assert_called_once()
