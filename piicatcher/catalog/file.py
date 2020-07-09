import json
import sys

from piicatcher.catalog import Store
from piicatcher.piitypes import PiiTypeEncoder


class FileStore(Store):
    @classmethod
    def save_schemas(cls, explorer):
        if explorer.catalog["file"] is not None:
            with open(explorer.catalog["file"], "w") as fh:
                json.dump(
                    explorer.get_dict(),
                    fh,
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
