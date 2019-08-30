from unittest import TestCase, mock
from argparse import Namespace

from piicatcher.files.explorer import File, FileExplorer, dispatch
from piicatcher.piitypes import PiiTypes


class TestDispatcher(TestCase):
    def test_file_dispatch(self):
        with mock.patch('piicatcher.files.explorer.FileExplorer.scan', autospec=True) as mock_scan_method:
            with mock.patch('piicatcher.files.explorer.FileExplorer.get_tabular', autospec=True) as mock_tabular_method:
                with mock.patch('piicatcher.files.explorer.tableprint', autospec=True) as MockTablePrint:
                    dispatch(Namespace(path='/a/b/c', output_format='ascii_table'))
                    mock_scan_method.assert_called_once()
                    mock_tabular_method.assert_called_once()
                    MockTablePrint.table.assert_called_once()


class TestExplorer(TestCase):
    def test_tabular(self):
        explorer = FileExplorer("/tmp")
        f1 = File("/tmp/1", "text/plain")
        f1._pii.add(PiiTypes.BIRTH_DATE)

        f2 = File("/tmp/2", "application/pdf")
        f2._pii.add(PiiTypes.UNSUPPORTED)

        explorer._files.append(f1)
        explorer._files.append(f2)

        self.assertEqual([
            ['/tmp/1', 'text/plain', '[{"__enum__": "PiiTypes.BIRTH_DATE"}]'],
            ['/tmp/2', 'application/pdf', '[{"__enum__": "PiiTypes.UNSUPPORTED"}]']
        ], explorer.get_tabular())

