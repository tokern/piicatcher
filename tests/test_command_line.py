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

    @patch('piicatcher.explorer.databases.Explorer')
    def test_host(self, explorer):
        runner = CliRunner()
        result = runner.invoke(cli, ["db", "-s", "connection_string"])
        self.assertEqual(0, result.exit_code)
        explorer.dispatch.assert_called_once_with(Namespace(connection_type='mysql',
                                                            database='',
                                                            host='connection_string',
                                                            list_all=False,
                                                            output=None,
                                                            output_format='ascii_table',
                                                            password=None,
                                                            port=None,
                                                            scan_type='shallow',
                                                            user=None,
                                                            orm={'host': None, 'port': None,
                                                                 'user': None, 'password': None}))

    @patch('piicatcher.explorer.databases.Explorer')
    def test_port(self, explorer):
        runner = CliRunner()
        result = runner.invoke(cli, ["db", "-s", "connection_string", "-R", "6032"])
        self.assertEqual("", result.stdout)
        self.assertEqual(0, result.exit_code)
        explorer.dispatch.assert_called_once_with(Namespace(connection_type='mysql',
                                                            database='',
                                                            host='connection_string',
                                                            list_all=False,
                                                            output=None,
                                                            output_format='ascii_table',
                                                            password=None,
                                                            port=6032,
                                                            scan_type='shallow',
                                                            user=None,
                                                            orm={'host': None, 'port': None, 
                                                                 'user': None, 'password': None}))

    @patch('piicatcher.explorer.databases.Explorer')
    def test_host_user_password(self, explorer):
        runner = CliRunner()
        result = runner.invoke(cli, ["db", "-s", "connection_string", "-u", "user", "-p", "passwd"])
        self.assertEqual("", result.stdout)
        self.assertEqual(0, result.exit_code)
        explorer.dispatch.assert_called_once_with(Namespace(connection_type='mysql',
                                                            database='',
                                                            host='connection_string',
                                                            list_all=False,
                                                            output=None,
                                                            output_format='ascii_table',
                                                            password='passwd',
                                                            port=None,
                                                            scan_type='shallow',
                                                            user='user',
                                                            orm={'host': None, 'port': None,
                                                                 'user': None, 'password': None}))

    @patch('piicatcher.explorer.databases.Explorer')
    def test_deep_scan_type(self, explorer):
        runner = CliRunner()
        result = runner.invoke(cli, ["db", "-s", "connection_string", "-c", "deep"])
        self.assertEqual("", result.stdout)
        self.assertEqual(0, result.exit_code)
        explorer.dispatch.assert_called_once_with(Namespace(connection_type='mysql',
                                                            database='',
                                                            host='connection_string',
                                                            list_all=False,
                                                            output=None,
                                                            output_format='ascii_table',
                                                            password=None,
                                                            port=None,
                                                            scan_type='deep',
                                                            user=None,
                                                            orm={'host': None, 'port': None, 
                                                                 'user': None, 'password': None}))

    @patch('piicatcher.explorer.databases.Explorer')
    def test_deep_scan_type(self, explorer):
        runner = CliRunner()
        result = runner.invoke(cli, ["db", "-s", "connection_string", "-c", "shallow"])
        self.assertEqual("", result.stdout)
        self.assertEqual(0, result.exit_code)
        explorer.dispatch.assert_called_once_with(Namespace(connection_type='mysql',
                                                            database='',
                                                            host='connection_string',
                                                            list_all=False,
                                                            output=None,
                                                            output_format='ascii_table',
                                                            password=None,
                                                            port=None,
                                                            scan_type='shallow',
                                                            user=None, 
                                                            orm={'host': None, 'port': None, 
                                                                 'user': None, 'password': None}))


class TestSqliteParser(TestCase):
    def test_path_required(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["sqlite"])
        self.assertNotEqual(0, result.exit_code)
        self.assertEqual(
            'Usage: cli sqlite [OPTIONS]\nTry "cli sqlite --help" for help.\n\nError: Missing option "-s" / "--path".\n'
            , result.stdout)

    @patch('piicatcher.explorer.sqlite.Explorer')
    def test_host(self, explorer):
        runner = CliRunner()
        result = runner.invoke(cli, ["sqlite", "-s", "connection_string"])
        self.assertEqual("", result.stdout)
        self.assertEqual(0, result.exit_code)
        explorer.dispatch.assert_called_once_with(Namespace(
            list_all=False,
            output=None,
            output_format='ascii_table',
            path='connection_string',
            scan_type='shallow',
            orm={'host': None, 'port': None, 'user': None, 'password': None}))


class TestAWSParser(TestCase):

    @patch('piicatcher.explorer.aws.Explorer')
    def test_host_user_password(self, explorer):
        runner = CliRunner()
        result = runner.invoke(cli, ["aws", "-a", "AAAA", "-s", "SSSS", "-d", "s3://dir", "-r", "us-east"])
        self.assertEqual("", result.stdout)
        self.assertEqual(0, result.exit_code)
        explorer.dispatch.assert_called_once_with(Namespace(
            access_key='AAAA',
            list_all=False,
            output=None,
            output_format='ascii_table',
            region='us-east',
            scan_type='shallow',
            secret_key='SSSS',
            staging_dir='s3://dir',
            orm={'host': None, 'port': None, 'user': None, 'password': None}))

