from typing import Any, Generator, Tuple

import pytest
from dbcat.catalog import Catalog, CatColumn, CatSchema, CatSource, CatTable
from sqlalchemy import create_engine

from piicatcher.dbinfo import get_dbinfo
from piicatcher.generators import (
    _get_query,
    _get_table_count,
    _row_generator,
    column_generator,
    data_generator,
)


@pytest.fixture(scope="module")
def sqlalchemy_engine(
    load_data_and_pull,
) -> Generator[Tuple[Catalog, CatSource, Any], None, None]:
    catalog, source_id = load_data_and_pull
    with catalog.managed_session:
        source = catalog.get_source_by_id(source_id)
        engine = create_engine(source.conn_string)
        with engine.connect() as conn:
            yield catalog, source, conn


@pytest.fixture(scope="module")
def load_source(load_data_and_pull) -> Generator[Tuple[Catalog, CatSource], None, None]:
    catalog, source_id = load_data_and_pull

    with catalog.managed_session:
        source = catalog.get_source_by_id(source_id)
        yield catalog, source


def test_column_generator(load_source):
    catalog, source = load_source

    count = 0
    for tpl in column_generator(catalog=catalog, source=source):
        count += 1

    assert count == 8


def test_column_generator_include_schema(load_source):
    catalog, source = load_source
    schemata = catalog.search_schema(source_like=source.name, schema_like="%")

    count = 0
    for tpl in column_generator(
        catalog=catalog, source=source, include_schema_regex_str=[schemata[0].name]
    ):
        count += 1

    assert count == 8


def test_column_generator_exclude_schema(load_source):
    catalog, source = load_source
    schemata = catalog.search_schema(source_like=source.name, schema_like="%")

    count = 0
    for tpl in column_generator(
        catalog=catalog, source=source, exclude_schema_regex_str=[schemata[0].name]
    ):
        count += 1

    assert count == 0


def test_column_generator_include_table(load_source):
    catalog, source = load_source

    count = 0
    for tpl in column_generator(
        catalog=catalog, source=source, include_table_regex_str=["full.*"]
    ):
        count += 1

    assert count == 2


def test_column_generator_exclude_table(load_source):
    catalog, source = load_source

    count = 0
    for tpl in column_generator(
        catalog=catalog, source=source, exclude_table_regex_str=["full.*"]
    ):
        count += 1

    assert count == 6


def test_get_table_count(sqlalchemy_engine):
    catalog, source, conn = sqlalchemy_engine
    schemata = catalog.search_schema(source_like=source.name, schema_like="%")
    table = catalog.get_table(
        source_name=source.name, schema_name=schemata[0].name, table_name="full_pii"
    )

    table_count = _get_table_count(
        schema=table.schema,
        table=table,
        dbinfo=get_dbinfo(source.source_type),
        connection=conn,
    )

    assert table_count == 2


def test_get_query(sqlalchemy_engine):
    catalog, source, conn = sqlalchemy_engine
    schemata = catalog.search_schema(source_like=source.name, schema_like="%")
    table = catalog.get_table(
        source_name=source.name, schema_name=schemata[0].name, table_name="full_pii"
    )
    query = _get_query(
        schema=schemata[0],
        table=table,
        column_list=catalog.get_columns_for_table(table),
        dbinfo=get_dbinfo(source.source_type),
        connection=conn,
    )

    if source.source_type == "mysql":
        assert query == """select `name`,`state` from piidb.full_pii"""
    elif source.source_type == "postgresql":
        assert query == """select "name","state" from public.full_pii"""
    elif source.source_type == "sqlite":
        assert query == """select "name","state" from full_pii"""


def test_get_sample_query(sqlalchemy_engine):
    catalog, source, conn = sqlalchemy_engine
    schemata = catalog.search_schema(source_like=source.name, schema_like="%")
    table = catalog.get_table(
        source_name=source.name, schema_name=schemata[0].name, table_name="full_pii"
    )
    query = _get_query(
        schema=schemata[0],
        table=table,
        column_list=catalog.get_columns_for_table(table),
        dbinfo=get_dbinfo(source.source_type),
        connection=conn,
        sample_size=1,
    )

    if source.source_type == "mysql":
        assert query == """select `name`,`state` from piidb.full_pii limit 1"""
    elif source.source_type == "postgresql":
        assert (
            query
            == """SELECT "name","state" FROM public.full_pii TABLESAMPLE BERNOULLI (10) LIMIT 1"""
        )
    elif source.source_type == "sqlite":
        assert query == """select "name","state" from full_pii"""


@pytest.mark.parametrize(
    ("source_type", "expected_query"),
    [
        (
            "redshift",
            'SELECT "column" FROM public.table ORDER BY RANDOM() LIMIT 1',
        ),
        ("snowflake", "SELECT column FROM public.table TABLESAMPLE BERNOULLI (1 ROWS)"),
        (
            "athena",
            'SELECT "column" FROM public.table TABLESAMPLE BERNOULLI (10) LIMIT 1',
        ),
    ],
)
def test_get_sample_query_redshift(mocker, source_type, expected_query):
    source = CatSource(name="src", source_type=source_type)
    schema = CatSchema(source=source, name="public")
    table = CatTable(schema=schema, name="table")
    column = CatColumn(table=table, name="column")

    mocker.patch("piicatcher.generators._get_table_count", return_value=100)
    query = _get_query(
        schema=schema,
        table=table,
        column_list=[column],
        dbinfo=get_dbinfo(source_type=source.source_type),
        connection=None,
        sample_size=1,
    )

    assert query == expected_query


def test_row_generator(sqlalchemy_engine):
    catalog, source, conn = sqlalchemy_engine
    schemata = catalog.search_schema(source_like=source.name, schema_like="%")
    table = catalog.get_table(
        source_name=source.name, schema_name=schemata[0].name, table_name="full_pii"
    )

    count = 0
    for row in _row_generator(
        source=source,
        schema=schemata[0],
        table=table,
        column_list=catalog.get_columns_for_table(table),
    ):
        count += 1
        assert row[0] is not None
        assert row[1] is not None

    assert count == 2


def test_data_generator(sqlalchemy_engine):
    catalog, source, conn = sqlalchemy_engine

    count = 0
    for tpl in data_generator(catalog=catalog, source=source):
        count += 1

    assert count == 14


def test_data_generator_include_schema(load_source):
    catalog, source = load_source
    schemata = catalog.search_schema(source_like=source.name, schema_like="%")

    count = 0
    for tpl in data_generator(
        catalog=catalog, source=source, include_schema_regex_str=[schemata[0].name]
    ):
        count += 1

    assert count == 14


def test_data_generator_exclude_schema(load_source):
    catalog, source = load_source
    schemata = catalog.search_schema(source_like=source.name, schema_like="%")

    count = 0
    for tpl in data_generator(
        catalog=catalog, source=source, exclude_schema_regex_str=[schemata[0].name]
    ):
        count += 1

    assert count == 0


def test_data_generator_include_table(load_source):
    catalog, source = load_source

    count = 0
    for tpl in data_generator(
        catalog=catalog, source=source, include_table_regex_str=["full.*"]
    ):
        count += 1

    assert count == 4


def test_data_generator_exclude_table(load_source):
    catalog, source = load_source

    count = 0
    for tpl in data_generator(
        catalog=catalog, source=source, exclude_table_regex_str=["full.*"]
    ):
        count += 1

    assert count == 10


def test_data_generator_include_int_table(load_source):
    catalog, source = load_source

    count = 0
    for tpl in data_generator(
        catalog=catalog, source=source, include_table_regex_str=["partial_data_type"]
    ):
        count += 1

    assert count == 2
