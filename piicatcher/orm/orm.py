import json

from piicatcher.orm.models import *
from piicatcher.piitypes import PiiTypes, PiiTypeEncoder

from piicatcher.db.explorer import Explorer


class Store:
    @classmethod
    def save_schemas(cls, explorer):
        with database_proxy.atomic():
            schemas = explorer.get_schemas()
            for s in schemas:
                schema_model = Schemas.create(name=s.get_name())

                for t in s.get_tables():
                    tbl_model = Tables.create(schema_id=schema_model, name=t.get_name())

                    for c in t.get_columns():
                        col_model = Columns.create(table_id=tbl_model, name=c.get_name(),
                                                   pii_type=json.dumps(list(c.get_pii_types()), cls=PiiTypeEncoder))
