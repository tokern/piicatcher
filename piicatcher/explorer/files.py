import json
import logging
import os
from argparse import Namespace
from typing import Any, Dict, Optional, TextIO

import click
import magic
import tableprint
from spacy.lang.en import English

from piicatcher.catalog.file import FileStore
from piicatcher.explorer.metadata import NamedObject
from piicatcher.piitypes import PiiTypeEncoder, PiiTypes
from piicatcher.scanner import NERScanner, RegexScanner


@click.command("files")
@click.pass_context
@click.option("--path", type=click.Path(), help="Path to file or directory")
def cli(ctx, path):
    ns = Namespace(path=path, catalog=ctx.obj["catalog"])

    FileExplorer.dispatch(ns)


class IO(NamedObject):
    def __init__(self, name: str, fd: Optional[TextIO] = None):
        super(IO, self).__init__(name, (), ())
        self.descriptor = fd

    @property
    def descriptor(self) -> Optional[TextIO]:
        return self._descriptor

    @descriptor.setter
    def descriptor(self, fd: TextIO) -> None:
        self._descriptor = fd

    def scan(self, context: Dict[str, Any]) -> None:
        tokenizer = context["tokenizer"]
        regex = context["regex"]
        ner = context["ner"]

        data = self._descriptor.read()
        [self._pii.add(pii) for pii in ner.scan(data)]
        tokens = tokenizer.tokenize(data)
        for t in tokens:
            if not t.is_stop:
                [self._pii.add(pii) for pii in regex.scan(t.text)]


class File(IO):
    def __init__(self, name, mime_type):
        super(File, self).__init__(name)
        self._mime_type = mime_type

    def get_mime_type(self):
        return self._mime_type

    def scan(self, context):
        if (
            not self._mime_type.startswith("text/")
            and self._mime_type != "application/csv"
        ):
            self._pii.add(PiiTypes.UNSUPPORTED)
        else:
            with open(self.get_name(), "r") as f:
                self.descriptor = f
                super().scan(context)


class FileExplorer:
    @classmethod
    def dispatch(cls, ns):
        logging.debug("File Dispatch entered")
        explorer = cls(ns)
        explorer.scan()

        if ns.catalog["format"] == "ascii_table":
            headers = ["Path", "Mime/Type", "pii"]
            tableprint.table(explorer.get_tabular(), headers)
        elif ns.catalog["format"] == "json":
            FileStore.save_schemas(explorer)

    def __init__(self, ns):
        self._path = ns.path
        self._files = []
        self.catalog = ns.catalog

    def scan(self):
        logging.debug("Scanning %s" % self._path)
        if os.path.isfile(self._path):
            mime_type = magic.from_file(self._path, mime=True)
            self._files.append(File(self._path, mime_type))
            logging.debug(
                "\t- full path: %s, mime_type: %s"
                % (os.path.abspath(self._path), mime_type)
            )
        else:
            for root, subdirs, files in os.walk(self._path):
                for filename in files:
                    file_path = os.path.join(root, filename)
                    mime_type = magic.from_file(file_path, mime=True)

                    logging.debug(
                        "\t- full path: %s, mime_type: %s" % (file_path, mime_type)
                    )
                    self._files.append(File(file_path, mime_type))

        context = {
            "tokenizer": Tokenizer(),
            "regex": RegexScanner(),
            "ner": NERScanner(),
        }
        for f in self._files:
            f.scan(context)

    def get_tabular(self):
        tabular = []
        for f in self._files:
            tabular.append(
                [
                    f.get_name(),
                    f.get_mime_type(),
                    json.dumps(list(f.get_pii_types()), cls=PiiTypeEncoder),
                ]
            )

        return tabular

    def get_dict(self):
        result = []
        for f in self._files:
            result.append(
                {
                    "path": f.get_name(),
                    "Mime/Type": f.get_mime_type(),
                    "pii": list(f.get_pii_types()),
                }
            )

        return {"files": result}


class Tokenizer:
    def __init__(self):
        nlp = English()
        # Create a Tokenizer with the default settings for English
        # including punctuation rules and exceptions
        self._tokenizer = nlp.tokenizer

    def tokenize(self, data):
        return self._tokenizer(data)
