import datetime
import logging
import re
from typing import Generator, List, Optional, Tuple, Type

from dbcat.catalog import Catalog, CatColumn, CatSchema, CatSource, CatTable
from dbcat.generators import NoMatchesError, table_generator
from sqlalchemy import create_engine, exc

from piicatcher.dbinfo import DbInfo, get_dbinfo

LOGGER = logging.getLogger(__name__)

SMALL_TABLE_MAX = 100


def column_generator(
    catalog: Catalog,
    source: CatSource,
    last_run: Optional[datetime.datetime] = None,
    include_schema_regex_str: List[str] = None,
    exclude_schema_regex_str: List[str] = None,
    include_table_regex_str: List[str] = None,
    exclude_table_regex_str: List[str] = None,
) -> Generator[Tuple[CatSchema, CatTable, CatColumn], None, None]:

    try:
        for schema, table in table_generator(
            catalog=catalog,
            source=source,
            include_schema_regex_str=include_schema_regex_str,
            exclude_schema_regex_str=exclude_schema_regex_str,
            include_table_regex_str=include_table_regex_str,
            exclude_table_regex_str=exclude_table_regex_str,
        ):

            for column in catalog.get_columns_for_table(
                table=table, newer_than=last_run
            ):
                LOGGER.debug(f"Scanning {schema.name}.{table.name}.{column.name}")
                yield schema, table, column
    except StopIteration:
        raise NoMatchesError


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

    LOGGER.debug(f"{len(matched_set)} text columns found")
    return list(matched_set)


def data_generator(
    catalog: Catalog,
    source: CatSource,
    last_run: Optional[datetime.datetime] = None,
    include_schema_regex_str: List[str] = None,
    exclude_schema_regex_str: List[str] = None,
    include_table_regex_str: List[str] = None,
    exclude_table_regex_str: List[str] = None,
) -> Generator[Tuple[CatSchema, CatTable, CatColumn, str], None, None]:

    for schema, table in table_generator(
        catalog=catalog,
        source=source,
        include_schema_regex_str=include_schema_regex_str,
        exclude_schema_regex_str=exclude_schema_regex_str,
        include_table_regex_str=include_table_regex_str,
        exclude_table_regex_str=exclude_table_regex_str,
    ):

        try:
            columns = _filter_text_columns(
                catalog.get_columns_for_table(table=table, newer_than=last_run)
            )
            if len(columns) > 0:
                for row in _row_generator(
                    column_list=columns, schema=schema, table=table, source=source
                ):
                    for col, val in zip(columns, row):
                        yield schema, table, col, val
        except StopIteration:
            raise NoMatchesError
        except exc.SQLAlchemyError as e:
            LOGGER.warning(
                f"Exception when getting data for {schema.name}.{table.name}. Code: {e.code}"
            )
