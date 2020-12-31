from argparse import Namespace
from unittest import mock

from piicatcher import scan_database
from piicatcher.explorer.aws import AthenaExplorer


def test_aws_dispatch():
    with mock.patch(
        "piicatcher.explorer.aws.AthenaExplorer.scan", autospec=True
    ) as mock_scan_method:
        with mock.patch(
            "piicatcher.explorer.aws.AthenaExplorer.get_tabular", autospec=True
        ) as mock_tabular_method:
            with mock.patch(
                "piicatcher.explorer.explorer.tableprint", autospec=True
            ) as mock_table_print:
                AthenaExplorer.dispatch(
                    Namespace(
                        access_key="ACCESS KEY",
                        secret_key="SECRET KEY",
                        staging_dir="s3://DIR",
                        region="us-east-1",
                        scan_type=None,
                        catalog={"format": "ascii_table"},
                        include_schema=(),
                        exclude_schema=(),
                        include_table=(),
                        exclude_table=(),
                        list_all=False,
                    )
                )
                mock_scan_method.assert_called_once()
                mock_tabular_method.assert_called_once()
                mock_table_print.table.assert_called_once()


def test_aws_api():
    with mock.patch(
        "piicatcher.explorer.aws.AthenaExplorer.scan", autospec=True
    ) as mock_scan_method:
        scan_database(None, "athena", "deep")
        mock_scan_method.assert_called_once()
