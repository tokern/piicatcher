from unittest.mock import ANY

from dbcat import Catalog
from pytest_cases import parametrize_with_cases
from typer.testing import CliRunner

import piicatcher
from piicatcher.api import ScanTypeEnum
from piicatcher.command_line import app


def case_sqlite_cli():
    return ["scan", "sqlite", "--name", "sqlite_cli", "--path", "/tmp/sqlite_path"]


def case_pg_cli():
    return [
        "scan",
        "postgresql",
        "--name",
        "pg_cli",
        "--username",
        "piicatcher",
        "--password",
        "pwd",
        "--uri",
        "127.0.0.1",
        "--database",
        "prod",
    ]


def case_mysql_cli():
    return [
        "scan",
        "mysql",
        "--name",
        "mysql_cli",
        "--username",
        "piicatcher",
        "--password",
        "pwd",
        "--uri",
        "127.0.0.1",
        "--database",
        "prod",
    ]


def case_redshift_cli():
    return [
        "scan",
        "redshift",
        "--name",
        "redshift_cli",
        "--username",
        "piicatcher",
        "--password",
        "pwd",
        "--uri",
        "127.0.0.1",
        "--database",
        "prod",
    ]


def case_snowflake_cli():
    return [
        "scan",
        "snowflake",
        "--name",
        "snowflake_cli",
        "--username",
        "piicatcher",
        "--password",
        "pwd",
        "--account",
        "account_name",
        "--warehouse",
        "warehouse_name",
        "--role",
        "role_name",
        "--database",
        "db",
    ]


def case_athena_cli():
    return [
        "scan",
        "athena",
        "--name",
        "athena_cli",
        "--aws-access-key-id",
        "key_id",
        "--aws-secret-access-key",
        "secret",
        "--region-name",
        "us-east-1",
        "--s3-staging-dir",
        "s3://dummy",
    ]


@parametrize_with_cases("args", cases=".")
def test_cli(mocker, temp_sqlite_path, args):
    mocker.patch("piicatcher.api.scan_database")
    mocker.patch.object(Catalog, "add_source")
    mocker.patch("piicatcher.cli.output")

    catalog_args = ["--catalog-path", temp_sqlite_path]
    runner = CliRunner()
    result = runner.invoke(app, catalog_args + args)
    print(result.stdout)
    assert result.exit_code == 0
    piicatcher.api.scan_database.assert_called_once()
    piicatcher.cli.output.assert_called_once()
    Catalog.add_source.assert_called_once()


@parametrize_with_cases("args", cases=".")
def test_include_exclude(mocker, temp_sqlite_path, args):
    mocker.patch("piicatcher.api.scan_database")
    mocker.patch.object(Catalog, "add_source")
    mocker.patch("piicatcher.cli.output")

    extended_args = args + [
        "--include-schema",
        "ischema",
        "--exclude-schema",
        "eschema",
        "--include-table",
        "itable",
        "--exclude-table",
        "etable",
    ]

    catalog_args = ["--catalog-path", temp_sqlite_path]
    runner = CliRunner()
    result = runner.invoke(app, catalog_args + extended_args)

    print(result.stdout)
    assert result.exit_code == 0
    piicatcher.api.scan_database.assert_called_once_with(
        catalog=ANY,
        exclude_schema_regex=("eschema",),
        exclude_table_regex=("etable",),
        include_schema_regex=("ischema",),
        include_table_regex=("itable",),
        scan_type=ScanTypeEnum.shallow,
        source=ANY,
    )
    piicatcher.cli.output.assert_called_once()
    Catalog.add_source.assert_called_once()


@parametrize_with_cases("args", cases=".")
def test_multiple_include_exclude(mocker, temp_sqlite_path, args):
    mocker.patch("piicatcher.api.scan_database")
    mocker.patch.object(Catalog, "add_source")
    mocker.patch("piicatcher.cli.output")

    extended_args = args + [
        "--include-schema",
        "ischema_1",
        "--include-schema",
        "ischema_2",
        "--exclude-schema",
        "eschema_1",
        "--exclude-schema",
        "eschema_2",
        "--include-table",
        "itable_1",
        "--include-table",
        "itable_2",
        "--exclude-table",
        "etable_1",
        "--exclude-table",
        "etable_2",
    ]

    catalog_args = ["--catalog-path", temp_sqlite_path]
    runner = CliRunner()
    result = runner.invoke(app, catalog_args + extended_args)

    print(result.stdout)
    assert result.exit_code == 0
    piicatcher.api.scan_database.assert_called_once_with(
        catalog=ANY,
        exclude_schema_regex=("eschema_1", "eschema_2"),
        exclude_table_regex=("etable_1", "etable_2"),
        include_schema_regex=("ischema_1", "ischema_2"),
        include_table_regex=("itable_1", "itable_2"),
        scan_type=ScanTypeEnum.shallow,
        source=ANY,
    )
    piicatcher.cli.output.assert_called_once()
    Catalog.add_source.assert_called_once()
