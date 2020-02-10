from unittest import TestCase
from unittest.mock import patch

from argparse import Namespace
from click.testing import CliRunner
from piicatcher.command_line import cli


class TestDbParser(TestCase):
    def test_host_required(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["db"])
        self.assertNotEqual(0, result.exit_code)
        self.assertEqual(
            'Usage: cli db [OPTIONS]\nTry "cli db --help" for help.\n\nError: Missing option "-s" / "--host".\n'
            , result.stdout)

    @patch('piicatcher.explorer.databases.RelDbExplorer')
    def test_host(self, explorer):
        runner = CliRunner()
        result = runner.invoke(cli, ["db", "-s", "connection_string"])
        self.assertEqual(0, result.exit_code)
        explorer.dispatch.assert_called_once_with(Namespace(connection_type='mysql',
                                                            database='',
                                                            host='connection_string',
                                                            list_all=False,
                                                            password=None,
                                                            port=None,
                                                            scan_type='shallow',
                                                            user=None,
                                                            include_schema=(),
                                                            exclude_schema=(),
                                                            include_table=(),
                                                            exclude_table=(),
                                                            catalog={'host': None, 'port': None,
                                                                     'user': None, 'password': None,
                                                                     'format': 'ascii_table',
                                                                     'file': None}))

    @patch('piicatcher.explorer.databases.RelDbExplorer')
    def test_port(self, explorer):
        runner = CliRunner()
        result = runner.invoke(cli, ["db", "-s", "connection_string", "-R", "6032"])
        self.assertEqual("", result.stdout)
        self.assertEqual(0, result.exit_code)
        explorer.dispatch.assert_called_once_with(Namespace(connection_type='mysql',
                                                            database='',
                                                            host='connection_string',
                                                            list_all=False,
                                                            password=None,
                                                            port=6032,
                                                            scan_type='shallow',
                                                            user=None,
                                                            include_schema=(),
                                                            exclude_schema=(),
                                                            include_table=(),
                                                            exclude_table=(),
                                                            catalog={'host': None, 'port': None,
                                                                     'user': None, 'password': None,
                                                                     'format': 'ascii_table',
                                                                     'file': None}))

    @patch('piicatcher.explorer.databases.RelDbExplorer')
    def test_host_user_password(self, explorer):
        runner = CliRunner()
        result = runner.invoke(cli, ["db", "-s", "connection_string", "-u", "user", "-p", "passwd"])
        self.assertEqual("", result.stdout)
        self.assertEqual(0, result.exit_code)
        explorer.dispatch.assert_called_once_with(Namespace(connection_type='mysql',
                                                            database='',
                                                            host='connection_string',
                                                            list_all=False,
                                                            password='passwd',
                                                            port=None,
                                                            scan_type='shallow',
                                                            user='user',
                                                            include_schema=(),
                                                            exclude_schema=(),
                                                            include_table=(),
                                                            exclude_table=(),
                                                            catalog={'host': None, 'port': None,
                                                                     'user': None, 'password': None,
                                                                     'format': 'ascii_table',
                                                                     'file': None}))

    @patch('piicatcher.explorer.databases.RelDbExplorer')
    def test_deep_scan_type(self, explorer):
        runner = CliRunner()
        result = runner.invoke(cli, ["db", "-s", "connection_string", "-c", "deep"])
        self.assertEqual("", result.stdout)
        self.assertEqual(0, result.exit_code)
        explorer.dispatch.assert_called_once_with(Namespace(connection_type='mysql',
                                                            database='',
                                                            host='connection_string',
                                                            list_all=False,
                                                            password=None,
                                                            port=None,
                                                            scan_type='deep',
                                                            user=None,
                                                            include_schema=(),
                                                            exclude_schema=(),
                                                            include_table=(),
                                                            exclude_table=(),
                                                            catalog={'host': None, 'port': None,
                                                                     'user': None, 'password': None,
                                                                     'format': 'ascii_table',
                                                                     'file': None}))

    @patch('piicatcher.explorer.databases.RelDbExplorer')
    def test_shallow_scan_type(self, explorer):
        runner = CliRunner()
        result = runner.invoke(cli, ["db", "-s", "connection_string", "-c", "shallow"])
        self.assertEqual("", result.stdout)
        self.assertEqual(0, result.exit_code)
        explorer.dispatch.assert_called_once_with(Namespace(connection_type='mysql',
                                                            database='',
                                                            host='connection_string',
                                                            list_all=False,
                                                            password=None,
                                                            port=None,
                                                            scan_type='shallow',
                                                            user=None,
                                                            include_schema=(),
                                                            exclude_schema=(),
                                                            include_table=(),
                                                            exclude_table=(),
                                                            catalog={'host': None, 'port': None,
                                                                     'user': None, 'password': None,
                                                                     'format': 'ascii_table',
                                                                     'file': None}))

    @patch('piicatcher.explorer.databases.RelDbExplorer')
    def test_include_exclude(self, explorer):
        runner = CliRunner()
        result = runner.invoke(cli, ["db", "-s", "connection_string",
                                     "-n", "include_schema",
                                     "-N", "exclude_schema",
                                     "-t", "include_table",
                                     "-T", "exclude_table"])

        self.assertEqual("", result.stdout)
        self.assertEqual(0, result.exit_code)
        explorer.dispatch.assert_called_once_with(Namespace(connection_type='mysql',
                                                            database='',
                                                            host='connection_string',
                                                            list_all=False,
                                                            password=None,
                                                            port=None,
                                                            scan_type='shallow',
                                                            user=None,
                                                            include_schema=("include_schema",),
                                                            exclude_schema=("exclude_schema",),
                                                            include_table=("include_table",),
                                                            exclude_table=("exclude_table",),
                                                            catalog={'host': None, 'port': None,
                                                                     'user': None, 'password': None,
                                                                     'format': 'ascii_table',
                                                                     'file': None}))

    @patch('piicatcher.explorer.databases.RelDbExplorer')
    def test_include_exclude_multiple(self, explorer):
        runner = CliRunner()
        result = runner.invoke(cli, ["db", "-s", "connection_string",
                                     "-n", "include_schema", "-n", "include_schema_2",
                                     "-N", "exclude_schema", "-N", "exclude_schema_2",
                                     "-t", "include_table", "-t", "include_table_2",
                                     "-T", "exclude_table", "-T", "exclude_table_2"])

        self.assertEqual("", result.stdout)
        self.assertEqual(0, result.exit_code)
        explorer.dispatch.assert_called_once_with(Namespace(connection_type='mysql',
                                                            database='',
                                                            host='connection_string',
                                                            list_all=False,
                                                            password=None,
                                                            port=None,
                                                            scan_type='shallow',
                                                            user=None,
                                                            include_schema=("include_schema", "include_schema_2"),
                                                            exclude_schema=("exclude_schema", "exclude_schema_2"),
                                                            include_table=("include_table", "include_table_2"),
                                                            exclude_table=("exclude_table", "exclude_table_2"),
                                                            catalog={'host': None, 'port': None,
                                                                     'user': None, 'password': None,
                                                                     'format': 'ascii_table',
                                                                     'file': None}))


class TestSqliteParser(TestCase):
    def test_path_required(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["sqlite"])
        self.assertNotEqual(0, result.exit_code)
        self.assertEqual(
            'Usage: cli sqlite [OPTIONS]\nTry "cli sqlite --help" for help.\n\nError: Missing option "-s" / "--path".\n'
            , result.stdout)

    @patch('piicatcher.explorer.sqlite.SqliteExplorer')
    def test_host(self, explorer):
        runner = CliRunner()
        result = runner.invoke(cli, ["sqlite", "-s", "connection_string",
                                     "-n", "include_schema",
                                     "-N", "exclude_schema",
                                     "-t", "include_table",
                                     "-T", "exclude_table"])
        self.assertEqual("", result.stdout)
        self.assertEqual(0, result.exit_code)
        explorer.dispatch.assert_called_once_with(Namespace(
            list_all=False,
            path='connection_string',
            scan_type='shallow',
            include_schema=("include_schema",),
            exclude_schema=("exclude_schema",),
            include_table=("include_table",),
            exclude_table=("exclude_table",),
            catalog={'host': None, 'port': None, 'user': None, 'password': None,
                     'format': 'ascii_table',
                     'file': None}))

    @patch('piicatcher.explorer.sqlite.SqliteExplorer')
    def test_include_exclude(self, explorer):
        runner = CliRunner()
        result = runner.invoke(cli, ["sqlite", "-s", "connection_string"])
        self.assertEqual("", result.stdout)
        self.assertEqual(0, result.exit_code)
        explorer.dispatch.assert_called_once_with(Namespace(
            list_all=False,
            path='connection_string',
            scan_type='shallow',
            include_schema=(),
            exclude_schema=(),
            include_table=(),
            exclude_table=(),
            catalog={'host': None, 'port': None, 'user': None, 'password': None,
                     'format': 'ascii_table',
                     'file': None}))

    @patch('piicatcher.explorer.sqlite.SqliteExplorer')
    def test_include_exclude_multiple(self, explorer):
        runner = CliRunner()
        result = runner.invoke(cli, ["sqlite", "-s", "connection_string",
                                     "-n", "include_schema", "-n", "include_schema_2",
                                     "-N", "exclude_schema", "-N", "exclude_schema_2",
                                     "-t", "include_table", "-t", "include_table_2",
                                     "-T", "exclude_table", "-T", "exclude_table_2"])
        self.assertEqual("", result.stdout)
        self.assertEqual(0, result.exit_code)
        explorer.dispatch.assert_called_once_with(Namespace(
            list_all=False,
            path='connection_string',
            scan_type='shallow',
            include_schema=("include_schema", "include_schema_2"),
            exclude_schema=("exclude_schema", "exclude_schema_2"),
            include_table=("include_table", "include_table_2"),
            exclude_table=("exclude_table", "exclude_table_2"),
            catalog={'host': None, 'port': None, 'user': None, 'password': None,
                     'format': 'ascii_table',
                     'file': None}))


class TestAWSParser(TestCase):

    @patch('piicatcher.explorer.aws.AthenaExplorer')
    def test_host_user_password(self, explorer):
        runner = CliRunner()
        result = runner.invoke(cli, ["aws", "-a", "AAAA", "-s", "SSSS", "-d", "s3://dir", "-r", "us-east",
                                     "-n", "include_schema", "-n", "include_schema_2",
                                     "-N", "exclude_schema", "-N", "exclude_schema_2",
                                     "-t", "include_table", "-t", "include_table_2",
                                     "-T", "exclude_table", "-T", "exclude_table_2"])
        self.assertEqual("", result.stdout)
        self.assertEqual(0, result.exit_code)
        explorer.dispatch.assert_called_once_with(Namespace(
            access_key='AAAA',
            list_all=False,
            region='us-east',
            scan_type='shallow',
            secret_key='SSSS',
            staging_dir='s3://dir',
            include_schema=("include_schema", "include_schema_2"),
            exclude_schema=("exclude_schema", "exclude_schema_2"),
            include_table=("include_table", "include_table_2"),
            exclude_table=("exclude_table", "exclude_table_2"),
            catalog={'host': None, 'port': None, 'user': None, 'password': None,
                     'format': 'ascii_table',
                     'file': None}))

