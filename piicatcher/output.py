import datetime
import json
from typing import Any, Dict, List, Optional

from dbcat.catalog import Catalog, CatSchema, CatSource, CatTable
from dbcat.catalog.models import PiiTypes

from piicatcher.generators import column_generator


# Ref: https://stackoverflow.com/questions/24481852/serialising-an-enum-member-to-json
class PiiTypeEncoder(json.JSONEncoder):
    # pylint: disable=method-hidden
    def default(self, obj):
        if type(obj) == PiiTypes:
            return {"__enum__": str(obj)}
        return json.JSONEncoder.default(self, obj)


def output_dict(
    catalog: Catalog,
    source: CatSource,
    list_all: bool = False,
    last_run: datetime.datetime = None,
    include_schema_regex: List[str] = None,
    exclude_schema_regex: List[str] = None,
    include_table_regex: List[str] = None,
    exclude_table_regex: List[str] = None,
) -> Dict[Any, Any]:
    current_schema: Optional[CatSchema] = None
    current_table: Optional[CatTable] = None

    source_dict = {"name": source.name, "schemata": []}
    schema_dict = {"name": "", "tables": []}

    table_dict = {"name": "", "columns": []}
    for schema, table, column in column_generator(
        catalog=catalog,
        source=source,
        last_run=last_run,
        exclude_schema_regex_str=exclude_schema_regex,
        include_schema_regex_str=include_schema_regex,
        exclude_table_regex_str=exclude_table_regex,
        include_table_regex_str=include_table_regex,
    ):
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
                    "pii_plugin": column.pii_plugin,
                }
            )
    if len(table_dict["columns"]) > 0 or list_all:
        schema_dict["tables"].append(table_dict)  # type: ignore
    if len(schema_dict["tables"]) > 0 or list_all:
        source_dict["schemata"].append(schema_dict)

    return source_dict if len(source_dict["schemata"]) > 0 or list_all else {}


def output_tabular(
    catalog: Catalog,
    source: CatSource,
    list_all: bool = False,
    last_run: datetime.datetime = None,
    include_schema_regex: List[str] = None,
    exclude_schema_regex: List[str] = None,
    include_table_regex: List[str] = None,
    exclude_table_regex: List[str] = None,
) -> List[Any]:
    tabular = []

    for schema, table, column in column_generator(
        catalog=catalog,
        source=source,
        last_run=last_run,
        exclude_schema_regex_str=exclude_schema_regex,
        include_schema_regex_str=include_schema_regex,
        exclude_table_regex_str=exclude_table_regex,
        include_table_regex_str=include_table_regex,
    ):
        if list_all or column.pii_type is not None:
            tabular.append(
                [
                    schema.name,
                    table.name,
                    column.name,
                    str(column.pii_type),
                    column.pii_plugin,
                ]
            )

    return tabular
