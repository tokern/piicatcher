from typing import Any, Generator, Tuple

import pytest
from dbcat import Catalog
from dbcat.catalog import CatSource
from sqlalchemy import create_engine

from piicatcher.dbinfo import get_dbinfo
from piicatcher.generators import (
    CatalogObject,
    _filter_objects,
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


def test_simple_schema_include(load_source):
    catalog, source = load_source
    schema_objects = [
        CatalogObject(s.name, s.id)
        for s in catalog.search_schema(source_like=source.name, schema_like="%")
    ]

    filtered = _filter_objects(
        include_regex_str=[schema_objects[0].name],
        exclude_regex_str=None,
        objects=schema_objects,
    )
    assert len(filtered) == 1


def test_simple_schema_exclude(load_source):
    catalog, source = load_source
    schema_objects = [
        CatalogObject(s.name, s.id)
        for s in catalog.search_schema(source_like=source.name, schema_like="%")
    ]

    filtered = _filter_objects(
        exclude_regex_str=[schema_objects[0].name],
        include_regex_str=None,
        objects=schema_objects,
    )
    assert len(filtered) == 0


def test_simple_schema_include_exclude(load_source):
    catalog, source = load_source
    schema_objects = [
        CatalogObject(s.name, s.id)
        for s in catalog.search_schema(source_like=source.name, schema_like="%")
    ]

    filtered = _filter_objects(
        include_regex_str=[schema_objects[0].name],
        exclude_regex_str=[schema_objects[0].name],
        objects=schema_objects,
    )
    assert len(filtered) == 0


def test_regex_schema_include(load_source):
    catalog, source = load_source
    schema_objects = [
        CatalogObject(s.name, s.id)
        for s in catalog.search_schema(source_like=source.name, schema_like="%")
    ]

    filtered = _filter_objects(
        include_regex_str=[".*"], exclude_regex_str=None, objects=schema_objects
    )
    assert len(filtered) == 1


def test_regex_schema_exclude(load_source):
    catalog, source = load_source
    schema_objects = [
        CatalogObject(s.name, s.id)
        for s in catalog.search_schema(source_like=source.name, schema_like="%")
    ]

    filtered = _filter_objects(
        exclude_regex_str=[".*"], include_regex_str=None, objects=schema_objects
    )
    assert len(filtered) == 0


def test_regex_failed_schema_include(load_source):
    catalog, source = load_source
    schema_objects = [
        CatalogObject(s.name, s.id)
        for s in catalog.search_schema(source_like=source.name, schema_like="%")
    ]

    filtered = _filter_objects(
        include_regex_str=["fail.*"], exclude_regex_str=None, objects=schema_objects
    )
    assert len(filtered) == 0


def test_regex_failed_schema_exclude(load_source):
    catalog, source = load_source
    schema_objects = [
        CatalogObject(s.name, s.id)
        for s in catalog.search_schema(source_like=source.name, schema_like="%")
    ]

    filtered = _filter_objects(
        exclude_regex_str=["fail.*"], include_regex_str=None, objects=schema_objects
    )
    assert len(filtered) == 1


def test_regex_success_table_include(load_source):
    catalog, source = load_source
    table_objects = [
        CatalogObject(t.name, t.id)
        for t in catalog.search_tables(
            source_like=source.name, schema_like="%", table_like="%"
        )
    ]

    filtered = _filter_objects(
        include_regex_str=["full.*", "partial.*"],
        exclude_regex_str=None,
        objects=table_objects,
    )
    assert len(filtered) == 3
    assert sorted([c.name for c in filtered]) == [
        "full_pii",
        "partial_data_type",
        "partial_pii",
    ]


def test_regex_success_table_exclude(load_source):
    catalog, source = load_source
    table_objects = [
        CatalogObject(t.name, t.id)
        for t in catalog.search_tables(
            source_like=source.name, schema_like="%", table_like="%"
        )
    ]

    filtered = _filter_objects(
        exclude_regex_str=["full.*", "partial.*"],
        include_regex_str=None,
        objects=table_objects,
    )
    assert len(filtered) == 1
    assert [c.name for c in filtered] == ["no_pii"]


def test_regex_success_table_include_exclude(load_source):
    catalog, source = load_source
    table_objects = [
        CatalogObject(t.name, t.id)
        for t in catalog.search_tables(
            source_like=source.name, schema_like="%", table_like="%"
        )
    ]

    filtered = _filter_objects(
        include_regex_str=["full.*", "partial.*"],
        exclude_regex_str=["full.*"],
        objects=table_objects,
    )
    assert len(filtered) == 2
    assert sorted([c.name for c in filtered]) == ["partial_data_type", "partial_pii"]


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
        sample_boundary=1,
    )

    if source.source_type == "mysql":
        assert query == """select `name`,`state` from piidb.full_pii limit 10"""
    elif source.source_type == "postgresql":
        assert (
            query
            == """select "name","state" from public.full_pii TABLESAMPLE BERNOULLI (10)"""
        )
    elif source.source_type == "sqlite":
        assert query == """select "name","state" from full_pii"""


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
