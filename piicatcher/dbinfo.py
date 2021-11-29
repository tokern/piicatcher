from abc import ABC, abstractmethod
from typing import List, Type


class DbInfo(ABC):
    _query_template = "select {column_list} from {schema_name}.{table_name}"
    _count_query = "select count(*) from {schema_name}.{table_name}"
    _column_escape = '"'

    @classmethod
    def get_count_query(cls, schema_name: str, table_name: str) -> str:
        return cls._count_query.format(schema_name=schema_name, table_name=table_name)

    @classmethod
    def get_select_query(
        cls, schema_name: str, table_name: str, column_list: List[str]
    ) -> str:
        return cls._query_template.format(
            column_list="{col_list}".format(
                col_list=",".join(
                    "{escape}{name}{escape}".format(name=col, escape=cls._column_escape)
                    for col in column_list
                ),
            ),
            schema_name=schema_name,
            table_name=table_name,
        )

    @classmethod
    @abstractmethod
    def get_sample_query(cls, schema_name, table_name, column_list, num_rows) -> str:
        pass


class Sqlite(DbInfo):
    _query_template = "select {column_list} from {table_name}"
    _count_query = "select count(*) from {table_name}"

    @classmethod
    def get_select_query(
        cls, schema_name: str, table_name: str, column_list: List[str]
    ) -> str:
        return cls._query_template.format(
            column_list='"{0}"'.format('","'.join(col for col in column_list)),
            table_name=table_name,
        )

    @classmethod
    def get_sample_query(cls, schema_name, table_name, column_list, num_rows) -> str:
        raise NotImplementedError


class MySQL(DbInfo):
    _sample_query_template = (
        "select {column_list} from {schema_name}.{table_name} limit {num_rows}"
    )
    _column_escape = "`"

    @classmethod
    def get_sample_query(cls, schema_name, table_name, column_list, num_rows) -> str:
        return cls._sample_query_template.format(
            column_list="`{0}`".format("`,`".join(col for col in column_list)),
            schema_name=schema_name,
            table_name=table_name,
            num_rows=num_rows,
        )


class Postgres(DbInfo):
    _sample_query_template = "SELECT {column_list} FROM {schema_name}.{table_name} TABLESAMPLE BERNOULLI (10) LIMIT {num_rows}"

    @classmethod
    def get_sample_query(
        cls, schema_name: str, table_name: str, column_list: List[str], num_rows
    ) -> str:
        return cls._sample_query_template.format(
            column_list='"{0}"'.format('","'.join(col for col in column_list)),
            schema_name=schema_name,
            table_name=table_name,
            num_rows=num_rows,
        )


class Redshift(Postgres):
    _sample_query_template = "SELECT {column_list} FROM {schema_name}.{table_name} TABLESAMPLE BERNOULLI (10) LIMIT {num_rows}"


class Snowflake(DbInfo):
    _sample_query_template = "SELECT {column_list} FROM {schema_name}.{table_name} TABLESAMPLE BERNOULLI ({num_rows} ROWS)"

    @classmethod
    def get_sample_query(
        cls, schema_name: str, table_name: str, column_list: List[str], num_rows
    ) -> str:
        return cls._sample_query_template.format(
            column_list=",".join(column_list),
            schema_name=schema_name,
            table_name=table_name,
            num_rows=num_rows,
        )


class Athena(Postgres):
    pass


def get_dbinfo(source_type: str) -> Type[DbInfo]:
    if source_type == "sqlite":
        return Sqlite
    elif source_type == "mysql":
        return MySQL
    elif source_type == "postgresql":
        return Postgres
    elif source_type == "redshift":
        return Redshift
    elif source_type == "snowflake":
        return Snowflake
    elif source_type == "athena":
        return Athena
    raise AttributeError
