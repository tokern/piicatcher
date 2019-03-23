from unittest import TestCase, mock
from piicatcher.command_line import get_parser, dispatch

from argparse import ArgumentParser, Namespace


class ErrorRaisingArgumentParser(ArgumentParser):
    def error(self, message):
        raise ValueError(message)  # reraise an error


class TestParser(TestCase):
    def test_connection_required(self):
        with self.assertRaises(ValueError):
            get_parser(ErrorRaisingArgumentParser).parse_args([])

    def test_connection(self):
        ns = get_parser().parse_args(["-c", "connection_string"])
        self.assertEqual("connection_string", ns.connection)

    def test_default_console(self):
        ns = get_parser().parse_args(["-c", "connection_string"])
        print(ns)
        self.assertIsNone(ns.output)
        self.assertEqual("ascii_table", ns.output_format)


class TestDispatcher(TestCase):

    def test_basic_dispatch(self):
        with mock.patch('piicatcher.command_line.SqliteExplorer.scan', autospec=True) as mock_scan_method:
            with mock.patch('piicatcher.command_line.SqliteExplorer.get_tabular', autospec=True) as mock_tabular_method:
                with mock.patch('piicatcher.command_line.tableprint', autospec=True) as MockTablePrint:
                    dispatch(Namespace(connection='connection', output_format='ascii_table'))
                    mock_scan_method.assert_called_once()
                    mock_tabular_method.assert_called_once()
                    MockTablePrint.table.assert_called_once()
