from unittest import TestCase, mock
from argparse import Namespace

from piicatcher.explorer.aws import AthenaExplorer


class AwsExplorerTest(TestCase):
    def test_aws_dispath(self):
        with mock.patch('piicatcher.explorer.aws.AthenaExplorer.scan', autospec=True) as mock_scan_method:
            with mock.patch('piicatcher.explorer.aws.AthenaExplorer.get_tabular',
                            autospec=True) as mock_tabular_method:
                with mock.patch('piicatcher.explorer.explorer.tableprint', autospec=True) as MockTablePrint:
                    AthenaExplorer.dispatch(Namespace(access_key='ACCESS KEY',
                                                      secret_key='SECRET KEY',
                                                      staging_dir='s3://DIR',
                                                      region='us-east-1',
                                                      scan_type=None,
                                                      output_format="ascii_table",
                                                      catalog=None,
                                                      list_all=False))
                    mock_scan_method.assert_called_once()
                    mock_tabular_method.assert_called_once()
                    MockTablePrint.table.assert_called_once()
