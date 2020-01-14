import logging
from argparse import Namespace

from click.testing import CliRunner
from pytest_mock import mocker

from piicatcher.command_line import cli


def test_db(tmp_path, mocker, caplog):
    caplog.set_level(logging.DEBUG)
    config_file = tmp_path / "db_host.ini"
    config_file.write_text("""
[db]
host="localhost"
database="db"
port="6032"
user="user"
password="password"
scan_type="deep"
list_all=True
output_format="json"
"""
                           )

    logging.info("Config File: %s" % config_file)
    rel = mocker.patch("piicatcher.explorer.databases.RelDbExplorer")
    runner = CliRunner()
    result = runner.invoke(cli, ["--config", str(config_file), "db"])
    assert result.exception is None
    assert "" == result.stdout
    assert 0 == result.exit_code

    ns = Namespace(connection_type='mysql',
                   database='db',
                   host='localhost',
                   list_all=True,
                   output=None,
                   output_format='json',
                   password="password",
                   port=6032,
                   scan_type='deep',
                   user='user',
                   orm={'host': None, 'port': None,
                   'user': None, 'password': None})
    rel.dispatch.assert_called_once_with(ns)


def test_sqlite(tmp_path, mocker, caplog):
    caplog.set_level(logging.DEBUG)
    config_file = tmp_path / "db_host.ini"
    config_file.write_text("""
[sqlite]
path="sqlite.db"
scan_type="deep"
list_all=True
output_format="json"
"""
                           )

    logging.info("Config File: %s" % config_file)
    explorer = mocker.patch("piicatcher.explorer.sqlite.SqliteExplorer")
    runner = CliRunner()
    result = runner.invoke(cli, ["--config", str(config_file), "sqlite"])
    assert result.exception is None
    assert "" == result.stdout
    assert 0 == result.exit_code
    explorer.dispatch.assert_called_once_with(Namespace(path='sqlite.db',
                                                        list_all=True,
                                                        output=None,
                                                        output_format='json',
                                                        scan_type='deep',
                                                        orm={'host': None, 'port': None, 
                                                             'user': None, 'password': None}))


def test_files(tmp_path, mocker, caplog):
    caplog.set_level(logging.DEBUG)
    config_file = tmp_path / "db_host.ini"
    config_file.write_text("""
[files]
path="file path"
output_format="json"
"""
                           )

    logging.info("Config File: %s" % config_file)
    explorer = mocker.patch("piicatcher.explorer.files.FileExplorer")
    runner = CliRunner()
    result = runner.invoke(cli, ["--config", str(config_file), "files"])
    assert result.exception is None
    assert "" == result.stdout
    assert 0 == result.exit_code
    explorer.dispatch.assert_called_once_with(Namespace(path='file path',
                                                        output=None,
                                                        output_format='json'))


def test_aws(tmp_path, mocker, caplog):
    caplog.set_level(logging.DEBUG)
    config_file = tmp_path / "db_host.ini"
    config_file.write_text("""
[aws]
access_key='AAAA'
list_all=True
output_format='json'
region='us-east'
scan_type='deep'
secret_key='SSSS'
staging_dir='s3://dir'
""")

    logging.info("Config File: %s" % config_file)
    explorer = mocker.patch("piicatcher.explorer.aws.AthenaExplorer")
    runner = CliRunner()
    result = runner.invoke(cli, ["--config", str(config_file), "aws"])
    assert result.exception is None
    assert "" == result.stdout
    assert 0 == result.exit_code
    explorer.dispatch.assert_called_once_with(Namespace(
        access_key='AAAA',
        list_all=True,
        output=None,
        output_format='json',
        region='us-east',
        scan_type='deep',
        secret_key='SSSS',
        staging_dir='s3://dir',
        orm={'host': None, 'port': None, 'user': None, 'password': None}))

