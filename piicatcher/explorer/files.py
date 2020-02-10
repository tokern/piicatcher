from argparse import Namespace

import click
import tableprint
import json
import logging
import os
import magic

from piicatcher.explorer.metadata import NamedObject
from piicatcher.piitypes import PiiTypes, PiiTypeEncoder
from piicatcher.scanner import NERScanner, RegexScanner
from piicatcher.tokenizer import Tokenizer


@click.command('files')
@click.pass_context
@click.option("--path", type=click.Path(), help="Path to file or directory")
@click.option("--output", type=click.File(), default=None,
              help="DEPRECATED. Please use --catalog-file")
@click.option("--output-format", type=click.Choice(["ascii_table", "json", "db"]),
              help="DEPRECATED. Please use --catalog-format")
def cli(ctx, path, output, output_format):
    ns = Namespace(path=path,
                   catalog=ctx.obj['catalog'])

    if output_format is not None or output is not None:
        logging.warning("--output-format and --output is deprecated. "
                        "Please use --catalog-format and --catalog-file")

    if output_format is not None:
        ns.catalog['format'] = output_format

    if output is not None:
        ns.catalog['file'] = output

    logging.debug(vars(ns))
    FileExplorer.dispatch(ns)


class File(NamedObject):

    def __init__(self, name, mime_type):
        super(File, self).__init__(name, (), ())
        self._mime_type = mime_type

    def get_mime_type(self):
        return self._mime_type

    def scan(self, context):
        tokenizer = context['tokenizer']
        regex = context['regex']
        ner = context['ner']

        if not self._mime_type.startswith('text/'):
            self._pii.add(PiiTypes.UNSUPPORTED)
        else:
            with open(self.get_name(), 'r') as f:
                data = f.read()
                [self._pii.add(pii) for pii in ner.scan(data)]
                tokens = tokenizer.tokenize(data)
                for t in tokens:
                    if not t.is_stop:
                        [self._pii.add(pii) for pii in regex.scan(t.text)]


class FileExplorer:
    @classmethod
    def dispatch(cls, ns):
        logging.debug("File Dispatch entered")
        explorer = FileExplorer(ns.path)
        explorer.scan()

        if ns.catalog['format'] == "ascii_table":
            headers = ["Path", "Mime/Type", "pii"]
            tableprint.table(explorer.get_tabular(), headers)
        elif ns.catalog['format'] == "json":
            print(json.dumps(explorer.get_dict(), sort_keys=True, indent=2, cls=PiiTypeEncoder))

    def __init__(self, path):
        self._path = path
        self._files = []

    def scan(self):
        logging.debug("Scanning %s" % self._path)
        if os.path.isfile(self._path):
            mime_type = magic.from_file(self._path, mime=True)
            self._files.append(File(self._path, mime_type))
            logging.debug('\t- full path: %s, mime_type: %s' % (os.path.abspath(self._path), mime_type))
        else:
            for root, subdirs, files in os.walk(self._path):
                for filename in files:
                    file_path = os.path.join(root, filename)
                    mime_type = magic.from_file(file_path, mime=True)

                    logging.debug('\t- full path: %s, mime_type: %s' % (file_path, mime_type))
                    self._files.append(File(file_path, mime_type))

        context = {'tokenizer': Tokenizer(), 'regex': RegexScanner(), 'ner': NERScanner()}
        for f in self._files:
            f.scan(context)

    def get_tabular(self):
        tabular = []
        for f in self._files:
            tabular.append([f.get_name(), f.get_mime_type(), json.dumps(list(f.get_pii_types()),
                                                                        cls=PiiTypeEncoder)])

        return tabular

    def get_dict(self):
        result = []
        for f in self._files:
            result.append({
                'path': f.get_name(),
                'Mime/Type': f.get_mime_type(),
                'pii': list(f.get_pii_types())
            })

        return {'files': result}
