from unittest.mock import ANY

from dbcat.catalog import Catalog
from pytest_cases import parametrize_with_cases
from typer.testing import CliRunner

import piicatcher
import piicatcher.command_line
from piicatcher.api import OutputFormat, ScanTypeEnum
from piicatcher.command_line import app
from piicatcher.generators import SMALL_TABLE_MAX


def case_sqlite_cli():
    return ["detect", "--source-name", "db_cli"]


@parametrize_with_cases("args", cases=".")
def test_cli(mocker, temp_sqlite_path, args):
    mocker.patch("piicatcher.command_line.scan_database")
    mocker.patch.object(Catalog, "get_source")
    mocker.patch("piicatcher.command_line.str_output")

    catalog_args = ["--catalog-path", temp_sqlite_path]
    runner = CliRunner()
    result = runner.invoke(app, catalog_args + args)
    print(result.stdout)
    assert result.exit_code == 0
    piicatcher.command_line.scan_database.assert_called_once()
    piicatcher.command_line.str_output.assert_called_once()
    Catalog.get_source.assert_called_once_with("db_cli")


@parametrize_with_cases("args", cases=".")
def test_include_exclude(mocker, temp_sqlite_path, args):
    mocker.patch("piicatcher.command_line.scan_database")
    mocker.patch.object(Catalog, "get_source")
    mocker.patch("piicatcher.command_line.str_output")

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
    piicatcher.command_line.scan_database.assert_called_once_with(
        catalog=ANY,
        source=ANY,
        scan_type=ScanTypeEnum.shallow,
        incremental=True,
        output_format=OutputFormat.tabular,
        list_all=False,
        exclude_schema_regex=("eschema",),
        exclude_table_regex=("etable",),
        include_schema_regex=("ischema",),
        include_table_regex=("itable",),
        sample_size=SMALL_TABLE_MAX,
    )
    piicatcher.command_line.str_output.assert_called_once()
    Catalog.get_source.assert_called_once_with("db_cli")


@parametrize_with_cases("args", cases=".")
def test_multiple_include_exclude(mocker, temp_sqlite_path, args):
    mocker.patch("piicatcher.command_line.scan_database")
    mocker.patch.object(Catalog, "get_source")
    mocker.patch("piicatcher.command_line.str_output")

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
    piicatcher.command_line.scan_database.assert_called_once_with(
        catalog=ANY,
        source=ANY,
        scan_type=ScanTypeEnum.shallow,
        incremental=True,
        output_format=OutputFormat.tabular,
        list_all=False,
        exclude_schema_regex=("eschema_1", "eschema_2"),
        exclude_table_regex=("etable_1", "etable_2"),
        include_schema_regex=("ischema_1", "ischema_2"),
        include_table_regex=("itable_1", "itable_2"),
        sample_size=SMALL_TABLE_MAX,
    )
    piicatcher.command_line.str_output.assert_called_once()
    Catalog.get_source.assert_called_once_with("db_cli")


@parametrize_with_cases("args", cases=".")
def test_sample_size(mocker, temp_sqlite_path, args):
    mocker.patch("piicatcher.command_line.scan_database")
    mocker.patch.object(Catalog, "get_source")
    mocker.patch("piicatcher.command_line.str_output")

    extended_args = args + [
        "--sample-size",
        "10",
    ]

    catalog_args = ["--catalog-path", temp_sqlite_path]
    runner = CliRunner()
    result = runner.invoke(app, catalog_args + extended_args)

    print(result.stdout)
    assert result.exit_code == 0
    piicatcher.command_line.scan_database.assert_called_once_with(
        catalog=ANY,
        source=ANY,
        scan_type=ScanTypeEnum.shallow,
        incremental=True,
        output_format=OutputFormat.tabular,
        list_all=False,
        exclude_schema_regex=(),
        exclude_table_regex=(),
        include_schema_regex=(),
        include_table_regex=(),
        sample_size=10,
    )
    piicatcher.command_line.str_output.assert_called_once()
    Catalog.get_source.assert_called_once_with("db_cli")
