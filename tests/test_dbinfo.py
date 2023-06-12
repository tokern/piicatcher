import pytest
from dbcat.catalog import CatSchema, CatSource, CatTable

from piicatcher.dbinfo import BigQuery


@pytest.mark.parametrize(
    ("source_type", "expected_query"),
    [
        (
            "bigquery",
            "select count(*) from project.public.table",
        )
    ],
)
def test_get_count_query_bigquery(source_type, expected_query):
    source = CatSource(name="src", project_id="project", source_type=source_type)
    schema = CatSchema(source=source, name="public")
    table = CatTable(schema=schema, name="table")

    query = BigQuery.get_count_query(
        schema_name=schema.name, table_name=table.name, project_id=source.project_id
    )

    assert query == expected_query
