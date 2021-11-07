import json
from enum import Enum
from typing import Any, Dict, List, Optional

from dbcat import Catalog
from dbcat.catalog import CatSchema, CatSource, CatTable
from dbcat.catalog.models import PiiTypes
from tabulate import tabulate

from piicatcher.api import get_catalog
from piicatcher.app_state import app_state
from piicatcher.generators import column_generator


class OutputFormat(str, Enum):
    tabular = "tabular"
    json = "json"


# Ref: https://stackoverflow.com/questions/24481852/serialising-an-enum-member-to-json
class PiiTypeEncoder(json.JSONEncoder):
    # pylint: disable=method-hidden
    def default(self, obj):
        if type(obj) == PiiTypes:
            return {"__enum__": str(obj)}
        return json.JSONEncoder.default(self, obj)


def output(catalog_params: Dict[str, Any], source_name: str, list_all: bool) -> str:
    catalog = get_catalog(**catalog_params)

    with catalog.managed_session:
        source = catalog.get_source(source_name)

        if app_state["output_format"] == OutputFormat.tabular:
            table = output_tabular(catalog=catalog, source=source, list_all=list_all)
            return tabulate(
                tabular_data=table, headers=("schema", "table", "column", "PII Type")
            )
        else:
            d = output_dict(catalog=catalog, source=source, list_all=list_all)
            return json.dumps(d, sort_keys=True, indent=2, cls=PiiTypeEncoder)


def output_dict(catalog: Catalog, source: CatSource, list_all: bool) -> Dict[Any, Any]:
    current_schema: Optional[CatSchema] = None
    current_table: Optional[CatTable] = None

    source_dict = {"name": source.name, "schemata": []}
    schema_dict = {"name": "", "tables": []}

    table_dict = {"name": "", "columns": []}
    for schema, table, column in column_generator(catalog=catalog, source=source):
        if current_schema is None or schema != current_schema:
            if current_schema is not None:
                if len(table_dict["columns"]) > 0 or list_all:
                    schema_dict["tables"].append(table_dict)  # type: ignore
                if len(schema_dict["tables"]) > 0 or list_all:
                    source_dict["schemata"].append(schema_dict)
            current_schema = schema
            schema_dict = {"name": schema.name, "tables": []}
            current_table = None

        if current_table is None or table != current_table:
            if current_table is not None:
                if len(table_dict["columns"]) > 0 or list_all:
                    schema_dict["tables"].append(table_dict)  # type: ignore
            current_table = table
            table_dict = {"name": table.name, "columns": []}
        if column.pii_type is not None or list_all:
            table_dict["columns"].append(  # type: ignore
                {
                    "name": column.name,
                    "data_type": column.data_type,
                    "sort_order": column.sort_order,
                    "pii_type": column.pii_type,
                }
            )
    if len(table_dict["columns"]) > 0 or list_all:
        schema_dict["tables"].append(table_dict)  # type: ignore
    if len(schema_dict["tables"]) > 0 or list_all:
        source_dict["schemata"].append(schema_dict)

    return source_dict


def output_tabular(catalog: Catalog, source: CatSource, list_all: bool) -> List[Any]:
    tabular = []

    for schema, table, column in column_generator(catalog=catalog, source=source):
        if list_all or column.pii_type is not None:
            tabular.append([schema.name, table.name, column.name, str(column.pii_type)])

    return tabular
