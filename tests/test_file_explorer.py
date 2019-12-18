from unittest import TestCase, mock
from argparse import Namespace

from piicatcher.explorer.files import File, FileExplorer, dispatch
from piicatcher.piitypes import PiiTypes


class TestDispatcher(TestCase):
    def test_file_dispatch(self):
        with mock.patch('piicatcher.explorer.files.FileExplorer.scan', autospec=True) as mock_scan_method:
            with mock.patch('piicatcher.explorer.files.FileExplorer.get_tabular', autospec=True) as mock_tabular_method:
                with mock.patch('piicatcher.explorer.files.tableprint', autospec=True) as MockTablePrint:
                    dispatch(Namespace(path='/a/b/c', output_format='ascii_table'))
                    mock_scan_method.assert_called_once()
                    mock_tabular_method.assert_called_once()
                    MockTablePrint.table.assert_called_once()


class TestExplorer(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.explorer = FileExplorer("/tmp")
        f1 = File("/tmp/1", "text/plain")
        f1._pii.add(PiiTypes.BIRTH_DATE)

        f2 = File("/tmp/2", "application/pdf")
        f2._pii.add(PiiTypes.UNSUPPORTED)

        cls.explorer._files.append(f1)
        cls.explorer._files.append(f2)

    def test_tabular(self):
        self.assertEqual([
            ['/tmp/1', 'text/plain', '[{"__enum__": "PiiTypes.BIRTH_DATE"}]'],
            ['/tmp/2', 'application/pdf', '[{"__enum__": "PiiTypes.UNSUPPORTED"}]']
        ], self.explorer.get_tabular())

    def test_json(self):
        self.assertEqual({
            'files': [
                {'Mime/Type': 'text/plain', 'path': '/tmp/1', 'pii': [PiiTypes.BIRTH_DATE]},
                {'Mime/Type': 'application/pdf', 'path': '/tmp/2',  'pii': [PiiTypes.UNSUPPORTED]}
            ]
        }, self.explorer.get_dict())


class TestWalker(TestCase):

    def _check(self, result):
        self.assertEqual(len(result['files']), 1)
        self.assertEqual(result['files'][0]['Mime/Type'], 'text/plain')
        self.assertEqual(result['files'][0]['path'], 'tests/samples/sample-data.csv')
        self.assertEqual(len(result['files'][0]['pii']), 5)

    def testDirectory(self):
        explorer = FileExplorer("tests/samples")
        explorer.scan()
        result = explorer.get_dict()
        self._check(result)

    def testFile(self):
        explorer = FileExplorer("tests/samples/sample-data.csv")
        explorer.scan()
        result = explorer.get_dict()
        self._check(result)

