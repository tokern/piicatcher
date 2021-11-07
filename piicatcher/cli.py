from pathlib import Path
from typing import List, Optional

import typer

from piicatcher.api import (
    ScanTypeEnum,
    scan_athena,
    scan_mysql,
    scan_postgresql,
    scan_redshift,
    scan_snowflake,
    scan_sqlite,
)
from piicatcher.app_state import app_state
from piicatcher.output import output

app = typer.Typer()


schema_help_text = """
Scan only schemas matching schema; When this option is not specified, all
non-system schemas in the target database will be dumped. Multiple schemas can
be selected by writing multiple -n switches. Also, the schema parameter is
interpreted as a regular expression, so multiple schemas can also be selected
by writing wildcard characters in the pattern. When using wildcards, be careful
to quote the pattern if needed to prevent the shell from expanding the wildcards;
"""
exclude_schema_help_text = """
Do not scan any schemas matching the schema pattern. The pattern is interpreted
according to the same rules as for -n. -N can be given more than once to exclude
 schemas matching any of several patterns.

When both -n and -N are given, the behavior is to dump just the schemas that
match at least one -n switch but no -N switches. If -N appears without -n, then
schemas matching -N are excluded from what is otherwise a normal dump.")
"""
table_help_text = """
Dump only tables matching table. Multiple tables can be selected by writing
multiple -t switches. Also, the table parameter is interpreted as a regular
expression, so multiple tables can also be selected by writing wildcard
characters in the pattern. When using wildcards, be careful to quote the pattern
 if needed to prevent the shell from expanding the wildcards.

The -n and -N switches have no effect when -t is used, because tables selected
by -t will be dumped regardless of those switches.
"""
exclude_table_help_text = """
Do not dump any tables matching the table pattern. The pattern is interpreted
according to the same rules as for -t. -T can be given more than once to
exclude tables matching any of several patterns.

When both -t and -T are given, the behavior is to dump just the tables that
match at least one -t switch but no -T switches. If -T appears without -t, then
tables matching -T are excluded from what is otherwise a normal dump.
"""


# pylint: disable=too-many-arguments
@app.command()
def sqlite(
    name: str = typer.Option(..., help="A memorable name for the database"),
    path: Path = typer.Option(..., help="File path to SQLite database"),
    scan_type: ScanTypeEnum = typer.Option(
        ScanTypeEnum.shallow,
        help="Choose deep(scan data) or shallow(scan column names only)",
    ),
    list_all: bool = typer.Option(
        False,
        help="List all columns. By default only columns with PII information is listed",
    ),
    include_schema: Optional[List[str]] = typer.Option(None, help=schema_help_text),
    exclude_schema: Optional[List[str]] = typer.Option(
        None, help=exclude_schema_help_text
    ),
    include_table: Optional[List[str]] = typer.Option(None, help=table_help_text),
    exclude_table: Optional[List[str]] = typer.Option(
        None, help=exclude_table_help_text
    ),
):
    scan_sqlite(
        catalog_params=app_state["catalog_connection"],
        name=name,
        path=path,
        scan_type=scan_type,
        include_schema_regex=include_schema,
        exclude_schema_regex=exclude_schema,
        include_table_regex=include_table,
        exclude_table_regex=exclude_table,
    )

    typer.echo(
        message=output(
            catalog_params=app_state["catalog_connection"],
            source_name=name,
            list_all=list_all,
        )
    )


@app.command()
def postgresql(
    name: str = typer.Option(..., help="A memorable name for the database"),
    username: str = typer.Option(..., help="Username or role to connect database"),
    password: str = typer.Option(..., help="Password of username or role"),
    database: str = typer.Option(..., help="Database name"),
    uri: str = typer.Option(..., help="Hostname or URI of the database"),
    port: Optional[int] = typer.Option(None, help="Port number of the database"),
    scan_type: ScanTypeEnum = typer.Option(
        ScanTypeEnum.shallow,
        help="Choose deep(scan data) or shallow(scan column names only)",
    ),
    list_all: bool = typer.Option(
        False,
        help="List all columns. By default only columns with PII information is listed",
    ),
    include_schema: Optional[List[str]] = typer.Option(None, help=schema_help_text),
    exclude_schema: Optional[List[str]] = typer.Option(
        None, help=exclude_schema_help_text
    ),
    include_table: Optional[List[str]] = typer.Option(None, help=table_help_text),
    exclude_table: Optional[List[str]] = typer.Option(
        None, help=exclude_table_help_text
    ),
):
    scan_postgresql(
        catalog_params=app_state["catalog_connection"],
        name=name,
        username=username,
        password=password,
        database=database,
        uri=uri,
        port=port,
        scan_type=scan_type,
        include_schema_regex=include_schema,
        exclude_schema_regex=exclude_schema,
        include_table_regex=include_table,
        exclude_table_regex=exclude_table,
    )

    typer.echo(
        message=output(
            catalog_params=app_state["catalog_connection"],
            source_name=name,
            list_all=list_all,
        )
    )


@app.command()
def mysql(
    name: str = typer.Option(..., help="A memorable name for the database"),
    username: str = typer.Option(..., help="Username or role to connect database"),
    password: str = typer.Option(..., help="Password of username or role"),
    database: str = typer.Option(..., help="Database name"),
    uri: str = typer.Option(..., help="Hostname or URI of the database"),
    port: Optional[int] = typer.Option(None, help="Port number of the database"),
    scan_type: ScanTypeEnum = typer.Option(
        ScanTypeEnum.shallow,
        help="Choose deep(scan data) or shallow(scan column names only)",
    ),
    list_all: bool = typer.Option(
        False,
        help="List all columns. By default only columns with PII information is listed",
    ),
    include_schema: Optional[List[str]] = typer.Option(None, help=schema_help_text),
    exclude_schema: Optional[List[str]] = typer.Option(
        None, help=exclude_schema_help_text
    ),
    include_table: Optional[List[str]] = typer.Option(None, help=table_help_text),
    exclude_table: Optional[List[str]] = typer.Option(
        None, help=exclude_table_help_text
    ),
):
    scan_mysql(
        catalog_params=app_state["catalog_connection"],
        name=name,
        username=username,
        password=password,
        database=database,
        uri=uri,
        port=port,
        scan_type=scan_type,
        include_schema_regex=include_schema,
        exclude_schema_regex=exclude_schema,
        include_table_regex=include_table,
        exclude_table_regex=exclude_table,
    )

    typer.echo(
        message=output(
            catalog_params=app_state["catalog_connection"],
            source_name=name,
            list_all=list_all,
        )
    )


@app.command()
def redshift(
    name: str = typer.Option(..., help="A memorable name for the database"),
    username: str = typer.Option(..., help="Username or role to connect database"),
    password: str = typer.Option(..., help="Password of username or role"),
    database: str = typer.Option(..., help="Database name"),
    uri: str = typer.Option(..., help="Hostname or URI of the database"),
    port: Optional[int] = typer.Option(None, help="Port number of the database"),
    scan_type: ScanTypeEnum = typer.Option(
        ScanTypeEnum.shallow,
        help="Choose deep(scan data) or shallow(scan column names only)",
    ),
    list_all: bool = typer.Option(
        False,
        help="List all columns. By default only columns with PII information is listed",
    ),
    include_schema: Optional[List[str]] = typer.Option(None, help=schema_help_text),
    exclude_schema: Optional[List[str]] = typer.Option(
        None, help=exclude_schema_help_text
    ),
    include_table: Optional[List[str]] = typer.Option(None, help=table_help_text),
    exclude_table: Optional[List[str]] = typer.Option(
        None, help=exclude_table_help_text
    ),
):
    scan_redshift(
        catalog_params=app_state["catalog_connection"],
        name=name,
        username=username,
        password=password,
        database=database,
        uri=uri,
        port=port,
        scan_type=scan_type,
        include_schema_regex=include_schema,
        exclude_schema_regex=exclude_schema,
        include_table_regex=include_table,
        exclude_table_regex=exclude_table,
    )

    typer.echo(
        message=output(
            catalog_params=app_state["catalog_connection"],
            source_name=name,
            list_all=list_all,
        )
    )


@app.command()
def snowflake(
    name: str = typer.Option(..., help="A memorable name for the database"),
    username: str = typer.Option(..., help="Username or role to connect database"),
    password: str = typer.Option(..., help="Password of username or role"),
    database: str = typer.Option(..., help="Database name"),
    account: str = typer.Option(..., help="Snowflake Account Name"),
    warehouse: str = typer.Option(..., help="Snowflake Warehouse Name"),
    role: str = typer.Option(..., help="Snowflake Role Name"),
    scan_type: ScanTypeEnum = typer.Option(
        ScanTypeEnum.shallow,
        help="Choose deep(scan data) or shallow(scan column names only)",
    ),
    list_all: bool = typer.Option(
        False,
        help="List all columns. By default only columns with PII information is listed",
    ),
    include_schema: Optional[List[str]] = typer.Option(None, help=schema_help_text),
    exclude_schema: Optional[List[str]] = typer.Option(
        None, help=exclude_schema_help_text
    ),
    include_table: Optional[List[str]] = typer.Option(None, help=table_help_text),
    exclude_table: Optional[List[str]] = typer.Option(
        None, help=exclude_table_help_text
    ),
):
    scan_snowflake(
        catalog_params=app_state["catalog_connection"],
        name=name,
        username=username,
        password=password,
        database=database,
        account=account,
        warehouse=warehouse,
        role=role,
        scan_type=scan_type,
        include_schema_regex=include_schema,
        exclude_schema_regex=exclude_schema,
        include_table_regex=include_table,
        exclude_table_regex=exclude_table,
    )

    typer.echo(
        message=output(
            catalog_params=app_state["catalog_connection"],
            source_name=name,
            list_all=list_all,
        )
    )


@app.command()
def athena(
    name: str = typer.Option(..., help="A memorable name for the database"),
    aws_access_key_id: str = typer.Option(..., help="AWS Access Key"),
    aws_secret_access_key: str = typer.Option(..., help="AWS Secret Key"),
    region_name: str = typer.Option(..., help="AWS Region Name"),
    s3_staging_dir: str = typer.Option(..., help="S3 Staging Dir"),
    scan_type: ScanTypeEnum = typer.Option(
        ScanTypeEnum.shallow,
        help="Choose deep(scan data) or shallow(scan column names only)",
    ),
    list_all: bool = typer.Option(
        False,
        help="List all columns. By default only columns with PII information is listed",
    ),
    include_schema: Optional[List[str]] = typer.Option(None, help=schema_help_text),
    exclude_schema: Optional[List[str]] = typer.Option(
        None, help=exclude_schema_help_text
    ),
    include_table: Optional[List[str]] = typer.Option(None, help=table_help_text),
    exclude_table: Optional[List[str]] = typer.Option(
        None, help=exclude_table_help_text
    ),
):
    scan_athena(
        catalog_params=app_state["catalog_connection"],
        name=name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
        s3_staging_dir=s3_staging_dir,
        scan_type=scan_type,
        include_schema_regex=include_schema,
        exclude_schema_regex=exclude_schema,
        include_table_regex=include_table,
        exclude_table_regex=exclude_table,
    )

    typer.echo(
        message=output(
            catalog_params=app_state["catalog_connection"],
            source_name=name,
            list_all=list_all,
        )
    )
