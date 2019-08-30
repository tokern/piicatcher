import tableprint
import json
import logging
import os
import magic

from piicatcher.tokenizer import Tokenizer
from piicatcher.db.metadata import NamedObject
from piicatcher.piitypes import PiiTypes, PiiTypeEncoder
from piicatcher.scanner import NERScanner, RegexScanner


def dispatch(ns):
    logging.debug("File Dispatch entered")
    explorer = FileExplorer(ns.path)
    explorer.scan()

    if ns.output_format == "ascii_table":
        headers = ["Path", "Mime/Type", "pii"]
        tableprint.table(explorer.get_tabular(), headers)
    elif ns.output_format == "json":
        print(json.dumps(explorer.get_dict(), sort_keys=True, indent=2))


def parser(sub_parsers):
    sub_parser = sub_parsers.add_parser("files")

    sub_parser.add_argument("--path",
                            help="Path to file or directory")

    sub_parser.add_argument("--output", default=None,
                            help="File path for report. If not specified, "
                                 "then report is printed to sys.stdout")
    sub_parser.add_argument("--output-format", choices=["ascii_table", "json", "orm"],
                            default="ascii_table",
                            help="Choose output format type")
    sub_parser.set_defaults(func=dispatch)


class File(NamedObject):

    def __init__(self, name, mime_type):
        super(File, self).__init__(name)
        self._mime_type = mime_type

    def get_mime_type(self):
        return self._mime_type

    def scan(self, context):
        tokenizer = context

        if not self._mime_type.startswith('text/'):
            self._pii.add(PiiTypes.UNSUPPORTED)
        else:
            with open(self.get_name(), 'r') as f:
                data = f.read()
                tokens = tokenizer.tokenize(data)
                for t in tokens:
                    if not t.is_stop:
                        for scanner in [RegexScanner(), NERScanner()]:
                            [self._pii.add(pii) for pii in scanner.scan(t.text)]


class FileExplorer:
    def __init__(self, path):
        self._path = path
        self._files = []

    def scan(self):
        logging.debug("Scanning %s" % self._path)
        for root, subdirs, files in os.walk(self._path):
            for filename in files:
                file_path = os.path.join(root, filename)
                mime_type = magic.from_file(file_path, mime=True)

                logging.debug('\t- full path: %s, mime_type: %s' % (file_path, mime_type))
                self._files.append(File(file_path, mime_type))

        t = Tokenizer()
        for f in self._files:
            f.scan(t)

    def get_tabular(self):
        tabular = []
        for f in self._files:
            tabular.append([f.get_name(), f.get_mime_type(), json.dumps(list(f.get_pii_types()),
                                                                        cls=PiiTypeEncoder)])

        return tabular
