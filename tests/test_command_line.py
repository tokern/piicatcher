from unittest import TestCase, mock
from piicatcher.command_line import get_parser, dispatch

from argparse import ArgumentParser, Namespace


class ErrorRaisingArgumentParser(ArgumentParser):
    def error(self, message):
        raise ValueError(message)  # reraise an error


class TestParser(TestCase):
    def test_host_required(self):
        with self.assertRaises(ValueError):
            get_parser(ErrorRaisingArgumentParser).parse_args([])

    def test_host(self):
        ns = get_parser().parse_args(["-s", "connection_string"])
        self.assertEqual("connection_string", ns.host)

    def test_port(self):
        ns = get_parser().parse_args(["-s", "connection_string", "-R", "6032"])
        self.assertEqual("connection_string", ns.host)
        self.assertEqual("6032", ns.port)

    def test_host_user_password(self):
        ns = get_parser().parse_args(["-s", "connection_string", "-u", "user", "-p", "passwd"])
        self.assertEqual("connection_string", ns.host)
        self.assertEqual("user", ns.user)
        self.assertEqual("passwd", ns.password)

    def test_default_console(self):
        ns = get_parser().parse_args(["-s", "connection_string"])
        self.assertIsNone(ns.output)
        self.assertEqual("ascii_table", ns.output_format)

    def test_default_scan_type(self):
        ns = get_parser().parse_args(["-s", "connection_string"])
        self.assertIsNone(ns.scan_type)

    def test_deep_scan_type(self):
        ns = get_parser().parse_args(["-s", "connection_string", "-c", "deep"])
        self.assertEqual("deep", ns.scan_type)

    def test_default_scan_type(self):
        ns = get_parser().parse_args(["-s", "connection_string", "-c", "shallow"])
        self.assertEqual("shallow", ns.scan_type)


class TestDispatcher(TestCase):

    def test_sqlite_dispatch(self):
        with mock.patch('piicatcher.command_line.SqliteExplorer.scan', autospec=True) as mock_scan_method:
            with mock.patch('piicatcher.command_line.SqliteExplorer.get_tabular', autospec=True) as mock_tabular_method:
                with mock.patch('piicatcher.command_line.tableprint', autospec=True) as MockTablePrint:
                    dispatch(Namespace(host='connection', output_format='ascii_table', connection_type='sqlite',
                                       scan_type=None, port=None))
                    mock_scan_method.assert_called_once()
                    mock_tabular_method.assert_called_once()
                    MockTablePrint.table.assert_called_once()

    def test_mysql_dispatch(self):
        with mock.patch('piicatcher.command_line.MySQLExplorer.scan', autospec=True) as mock_scan_method:
            with mock.patch('piicatcher.command_line.MySQLExplorer.get_tabular', autospec=True) as mock_tabular_method:
                with mock.patch('piicatcher.command_line.tableprint', autospec=True) as MockTablePrint:
                    dispatch(Namespace(host='connection',
                                       port=None,
                                       output_format='ascii_table',
                                       connection_type='mysql',
                                       scan_type='deep',
                                       user='user',
                                       password='pass'))
                    mock_scan_method.assert_called_once()
                    mock_tabular_method.assert_called_once()
                    MockTablePrint.table.assert_called_once()

    def test_postgres_dispatch(self):
        with mock.patch('piicatcher.command_line.PostgreSQLExplorer.scan', autospec=True) as mock_scan_method:
            with mock.patch('piicatcher.command_line.PostgreSQLExplorer.get_tabular', autospec=True) as mock_tabular_method:
                with mock.patch('piicatcher.command_line.tableprint', autospec=True) as MockTablePrint:
                    dispatch(Namespace(host='connection',
                                       port=None,
                                       output_format='ascii_table',
                                       connection_type='postgres',
                                       scan_type=None,
                                       user='user',
                                       password='pass'))
                    mock_scan_method.assert_called_once()
                    mock_tabular_method.assert_called_once()
                    MockTablePrint.table.assert_called_once()

    def test_mysql_shallow_scan(self):
        with mock.patch('piicatcher.command_line.MySQLExplorer.shallow_scan', autospec=True) as mock_shallow_scan_method:
            with mock.patch('piicatcher.command_line.MySQLExplorer.get_tabular', autospec=True) as mock_tabular_method:
                with mock.patch('piicatcher.command_line.tableprint', autospec=True) as MockTablePrint:
                    dispatch(Namespace(host='connection',
                                       port=None,
                                       output_format='ascii_table',
                                       connection_type='mysql',
                                       user='user',
                                       password='pass',
                                       scan_type="shallow"))
                    mock_shallow_scan_method.assert_called_once()
                    mock_tabular_method.assert_called_once()
                    MockTablePrint.table.assert_called_once()


class TestOrmDispatch(TestCase):
    def test_all_missing_parameters(self):
        with mock.patch('piicatcher.command_line.MySQLExplorer.shallow_scan', autospec=True) as mock_shallow_scan_method:
            self.assertRaises(RuntimeError, lambda: dispatch(Namespace(host='connection',
                                                                       port=None,
                                                                       output_format='orm',
                                                                       connection_type='mysql',
                                                                       user='user',
                                                                       password='pass',
                                                                       scan_type='shallow_scan')))

    def test_positive_dispatch(self):
        with mock.patch('piicatcher.command_line.MySQLExplorer.shallow_scan', autospec=True) as mock_shallow_scan_method:
            with mock.patch('piicatcher.command_line.init', autospec=True) as mock_init_method:
                with mock.patch('piicatcher.command_line.Store', autospec=True) as MockStore:
                    dispatch(Namespace(host='connection',
                                       port=None,
                                       output_format='orm',
                                       connection_type='mysql',
                                       user='user',
                                       password='pass',
                                       scan_type="shallow",
                                       orm_host='orm_host',
                                       orm_port='orm_port',
                                       orm_user='orm_user',
                                       orm_pass='orm_pass'))
                    mock_shallow_scan_method.assert_called_once()
                    mock_init_method.assert_called_once()
                    MockStore.save_schemas.assert_called_once()

