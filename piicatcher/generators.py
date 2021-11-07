import logging
import re
from collections import namedtuple
from typing import Generator, List, Optional, Tuple, Type

from dbcat import Catalog
from dbcat.catalog import CatColumn, CatSchema, CatSource, CatTable
from sqlalchemy import create_engine

from piicatcher.dbinfo import DbInfo, get_dbinfo

LOGGER = logging.getLogger(__name__)

SMALL_TABLE_MAX = 100


CatalogObject = namedtuple("CatalogObject", ["name", "id"])


def _filter_objects(
    include_regex_str: Optional[List[str]],
    exclude_regex_str: Optional[List[str]],
    objects: List[CatalogObject],
) -> List[CatalogObject]:
    if include_regex_str is not None and len(include_regex_str) > 0:
        include_regex = [re.compile(exp, re.IGNORECASE) for exp in include_regex_str]
        matched_set = set()
        for regex in include_regex:
            matched_set |= set(
                list(filter(lambda m: regex.search(m.name) is not None, objects,))
            )

        objects = list(matched_set)

    if exclude_regex_str is not None and len(exclude_regex_str) > 0:
        exclude_regex = [re.compile(exp, re.IGNORECASE) for exp in exclude_regex_str]
        for regex in exclude_regex:
            objects = list(filter(lambda m: regex.search(m.name) is None, objects))

    return objects


def column_generator(
    catalog: Catalog,
    source: CatSource,
    include_schema_regex_str: List[str] = None,
    exclude_schema_regex_str: List[str] = None,
    include_table_regex_str: List[str] = None,
    exclude_table_regex_str: List[str] = None,
) -> Generator[Tuple[CatSchema, CatTable, CatColumn], None, None]:

    schemata = _filter_objects(
        include_schema_regex_str,
        exclude_schema_regex_str,
        [
            CatalogObject(s.name, s.id)
            for s in catalog.search_schema(source_like=source.name, schema_like="%")
        ],
    )

    for schema_object in schemata:
        schema = catalog.get_schema_by_id(schema_object.id)

        table_objects = _filter_objects(
            include_table_regex_str,
            exclude_table_regex_str,
            [
                CatalogObject(t.name, t.id)
                for t in catalog.search_tables(
                    source_like=source.name, schema_like=schema.name, table_like="%"
                )
            ],
        )

        for table_object in table_objects:
            table = catalog.get_table_by_id(table_object.id)

            for column in catalog.get_columns_for_table(table=table):
                yield schema, table, column


def _get_table_count(
    schema: CatSchema, table: CatTable, dbinfo: Type[DbInfo], connection
) -> int:
    count = dbinfo.get_count_query(schema.name, table.name)
    logging.debug("Count Query: %s" % count)

    result = connection.execute(count)
    row = result.fetchone()

    return int(row[0])


def _get_query(
    schema: CatSchema,
    table: CatTable,
    column_list: List[CatColumn],
    dbinfo: Type[DbInfo],
    connection,
    sample_boundary: int = SMALL_TABLE_MAX,
) -> str:
    count = _get_table_count(schema, table, dbinfo, connection)
    LOGGER.debug("No. of rows in {}.{} is {}".format(schema.name, table.name, count))
    column_name_list: List[str] = [col.name for col in column_list]
    query = dbinfo.get_select_query(schema.name, table.name, column_name_list)

    if count > sample_boundary:
        try:
            query = dbinfo.get_sample_query(schema.name, table.name, column_name_list)
            LOGGER.debug("Choosing a SAMPLE query as table size is big")
        except NotImplementedError:
            LOGGER.warning(
                "Sample Row is not implemented for %s" % dbinfo.__class__.__name__
            )
    return query


def _row_generator(
    source: CatSource, schema: CatSchema, table: CatTable, column_list: List[CatColumn]
):
    engine = create_engine(source.conn_string)
    with engine.connect() as conn:
        dbinfo = get_dbinfo(source.source_type)
        query = _get_query(
            schema=schema,
            table=table,
            column_list=column_list,
            dbinfo=dbinfo,
            connection=conn,
        )
        LOGGER.debug(query)
        result = conn.execute(query)
        row = result.fetchone()
        while row is not None:
            yield row
            row = result.fetchone()


def _filter_text_columns(column_list: List[CatColumn]) -> List[CatColumn]:
    data_type_regex = [
        re.compile(exp, re.IGNORECASE) for exp in [".*char.*", ".*text.*"]
    ]

    matched_set = set()
    for regex in data_type_regex:
        matched_set |= set(
            list(filter(lambda m: regex.search(m.data_type) is not None, column_list,))
        )

    return list(matched_set)


def data_generator(
    catalog: Catalog,
    source: CatSource,
    include_schema_regex_str: List[str] = None,
    exclude_schema_regex_str: List[str] = None,
    include_table_regex_str: List[str] = None,
    exclude_table_regex_str: List[str] = None,
) -> Generator[Tuple[CatSchema, CatTable, CatColumn, str], None, None]:

    schemata = _filter_objects(
        include_schema_regex_str,
        exclude_schema_regex_str,
        [
            CatalogObject(s.name, s.id)
            for s in catalog.search_schema(source_like=source.name, schema_like="%")
        ],
    )

    for schema_object in schemata:
        schema = catalog.get_schema_by_id(schema_object.id)

        table_objects = _filter_objects(
            include_table_regex_str,
            exclude_table_regex_str,
            [
                CatalogObject(t.name, t.id)
                for t in catalog.search_tables(
                    source_like=source.name, schema_like=schema.name, table_like="%"
                )
            ],
        )

        for table_object in table_objects:
            table = catalog.get_table_by_id(table_object.id)
            columns = _filter_text_columns(catalog.get_columns_for_table(table=table))
            for row in _row_generator(
                column_list=columns, schema=schema, table=table, source=source
            ):
                for col, val in zip(columns, row):
                    yield schema, table, col, val
