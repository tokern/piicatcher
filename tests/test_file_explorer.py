import json
import logging
from argparse import Namespace
from shutil import rmtree
from unittest import TestCase, mock

import pytest

from piicatcher import scan_file_object
from piicatcher.explorer.files import File, FileExplorer
from piicatcher.piitypes import PiiTypes


class TestDispatcher(TestCase):
    def test_file_dispatch(self):
        with mock.patch(
            "piicatcher.explorer.files.FileExplorer.scan", autospec=True
        ) as mock_scan_method:
            with mock.patch(
                "piicatcher.explorer.files.FileExplorer.get_tabular", autospec=True
            ) as mock_tabular_method:
                with mock.patch(
                    "piicatcher.explorer.files.tableprint", autospec=True
                ) as MockTablePrint:
                    FileExplorer.dispatch(
                        Namespace(path="/a/b/c", catalog={"format": "ascii_table"})
                    )
                    mock_scan_method.assert_called_once()
                    mock_tabular_method.assert_called_once()
                    MockTablePrint.table.assert_called_once()


class TestWalker(TestCase):
    def _check(self, result):
        self.assertEqual(result["files"][0]["Mime/Type"], "text/plain")
        self.assertEqual(result["files"][0]["path"], "tests/samples/sample-data.csv")
        self.assertEqual(len(result["files"][0]["pii"]), 5)

    def testDirectory(self):
        explorer = FileExplorer(
            Namespace(path="tests/samples", catalog={"format": "ascii_table"})
        )
        explorer.scan()
        result = explorer.get_dict()
        self.assertEqual(len(result["files"]), 2)
        self._check(result)

    def testFile(self):
        explorer = FileExplorer(
            Namespace(
                path="tests/samples/sample-data.csv", catalog={"format": "ascii_table"}
            )
        )
        explorer.scan()
        result = explorer.get_dict()
        self.assertEqual(len(result["files"]), 1)
        self._check(result)


class MockFileExplorer(FileExplorer):
    def scan(self):
        f1 = File("/tmp/1", "text/plain")
        f1._pii.add(PiiTypes.BIRTH_DATE)

        f2 = File("/tmp/2", "application/pdf")
        f2._pii.add(PiiTypes.UNSUPPORTED)

        self._files.append(f1)
        self._files.append(f2)


@pytest.fixture(scope="module")
def namespace(request, tmpdir_factory):
    temp_dir = tmpdir_factory.mktemp("file_explorer_test")
    output_path = temp_dir.join("output.json")
    fh = open(output_path, "w")

    def finalizer():
        rmtree(temp_dir)
        logging.info("Deleted {}", str(temp_dir))

    request.addfinalizer(finalizer)

    return Namespace(
        path=temp_dir, catalog={"format": "json", "file": fh}, output_path=output_path
    )


def test_tabular(namespace):
    explorer = MockFileExplorer(namespace)
    explorer.scan()
    assert [
        ["/tmp/1", "text/plain", '[{"__enum__": "PiiTypes.BIRTH_DATE"}]'],
        ["/tmp/2", "application/pdf", '[{"__enum__": "PiiTypes.UNSUPPORTED"}]'],
    ] == explorer.get_tabular()


def test_dict(namespace):
    explorer = MockFileExplorer(namespace)
    explorer.scan()
    assert {
        "files": [
            {"Mime/Type": "text/plain", "path": "/tmp/1", "pii": [PiiTypes.BIRTH_DATE]},
            {
                "Mime/Type": "application/pdf",
                "path": "/tmp/2",
                "pii": [PiiTypes.UNSUPPORTED],
            },
        ]
    } == explorer.get_dict()


def test_output_json(request, namespace):
    MockFileExplorer.dispatch(namespace)
    namespace.catalog["file"].close()

    obj = json.load(namespace.output_path)
    assert obj == {
        "files": [
            {
                "Mime/Type": "text/plain",
                "path": "/tmp/1",
                "pii": [{"__enum__": "PiiTypes.BIRTH_DATE"}],
            },
            {
                "Mime/Type": "application/pdf",
                "path": "/tmp/2",
                "pii": [{"__enum__": "PiiTypes.UNSUPPORTED"}],
            },
        ]
    }


def test_file_object():
    with open("tests/samples/sample-data.csv", "r") as fd:
        pii_types = scan_file_object(fd)
        assert len(pii_types) == 5
