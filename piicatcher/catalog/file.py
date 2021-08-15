import json
import sys

from piicatcher.catalog import Store
from piicatcher.explorer.metadata import PiiTypeEncoder


class FileStore(Store):
    @classmethod
    def save_schemas(cls, explorer):
        if explorer.catalog["file"] is not None:
            json.dump(
                explorer.get_dict(),
                explorer.catalog["file"],
                sort_keys=True,
                indent=2,
                cls=PiiTypeEncoder,
            )
        else:
            json.dump(
                explorer.get_dict(),
                sys.stdout,
                sort_keys=True,
                indent=2,
                cls=PiiTypeEncoder,
            )
