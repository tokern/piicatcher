import json
from pathlib import Path
from typing import List, Optional

import typer
from tabulate import tabulate

from piicatcher.api import (
    OutputFormat,
    ScanTypeEnum,
    scan_athena,
    scan_mysql,
    scan_postgresql,
    scan_redshift,
    scan_snowflake,
    scan_sqlite,
)
from piicatcher.app_state import app_state
from piicatcher.generators import NoMatchesError
from piicatcher.output import PiiTypeEncoder

app = typer.Typer()


schema_help_text = """
Scan only schemas matching schema; When this option is not specified, all
non-system schemas in the target database will be dumped. Multiple schemas can
be selected by writing multiple --include switches. Also, the schema parameter is
interpreted as a regular expression, so multiple schemas can also be selected
by writing wildcard characters in the pattern. When using wildcards, be careful
to quote the pattern if needed to prevent the shell from expanding the wildcards;
"""
exclude_schema_help_text = """
Do not scan any schemas matching the schema pattern. The pattern is interpreted
according to the same rules as for --include. --exclude can be given more than once to exclude
 schemas matching any of several patterns.

When both --include and ---exclude are given, the behavior is to dump just the schemas that
match at least one --include switch but no --exclude switches.
If --exclude appears without --include, then schemas matching --exclude are excluded from what
is otherwise a normal scan.")
"""
table_help_text = """
Scan only tables matching table. Multiple tables can be selected by writing
multiple switches. Also, the table parameter is interpreted as a regular
expression, so multiple tables can also be selected by writing wildcard
characters in the pattern. When using wildcards, be careful to quote the pattern
 if needed to prevent the shell from expanding the wildcards.
"""
exclude_table_help_text = """
Do not scan any tables matching the table pattern. The pattern is interpreted
according to the same rules as for --include. --exclude can be given more than once to
exclude tables matching any of several patterns.

When both switches are given, the behavior is to dump just the tables that
match at least one --include switch but no --exclude switches. If --exclude appears without
--include, then tables matching --exclude are excluded from what is otherwise a normal scan.
"""


def str_output(op, output_format: OutputFormat):
    if output_format == OutputFormat.tabular:
        return tabulate(
            tabular_data=op,
            headers=("schema", "table", "column", "PII Type", "Scanner"),
        )
    else:
        return json.dumps(op, sort_keys=True, indent=2, cls=PiiTypeEncoder)


@app.command()
def sqlite(
    name: str = typer.Option(..., help="A memorable name for the database"),
    path: Path = typer.Option(..., help="File path to SQLite database"),
    scan_type: ScanTypeEnum = typer.Option(
        ScanTypeEnum.shallow,
        help="Choose deep(scan data) or shallow(scan column names only)",
    ),
    incremental: bool = typer.Option(
        True, help="Scan columns updated or created since last run",
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
    try:
        op = scan_sqlite(
            catalog_params=app_state["catalog_connection"],
            name=name,
            path=path,
            scan_type=scan_type,
            incremental=incremental,
            output_format=app_state["output_format"],
            list_all=list_all,
            include_schema_regex=include_schema,
            exclude_schema_regex=exclude_schema,
            include_table_regex=include_table,
            exclude_table_regex=exclude_table,
        )

        typer.echo(message=str_output(op, app_state["output_format"]))
    except NoMatchesError:
        typer.echo(message=NoMatchesError.message)
        typer.Exit(1)


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
    incremental: bool = typer.Option(
        True, help="Scan columns updated or created since last run",
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
    try:
        op = scan_postgresql(
            catalog_params=app_state["catalog_connection"],
            name=name,
            username=username,
            password=password,
            database=database,
            uri=uri,
            port=port,
            scan_type=scan_type,
            incremental=incremental,
            output_format=app_state["output_format"],
            list_all=list_all,
            include_schema_regex=include_schema,
            exclude_schema_regex=exclude_schema,
            include_table_regex=include_table,
            exclude_table_regex=exclude_table,
        )

        typer.echo(message=str_output(op, app_state["output_format"]))
    except NoMatchesError:
        typer.echo(message=NoMatchesError.message)
        typer.Exit(1)


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
    incremental: bool = typer.Option(
        True, help="Scan columns updated or created since last run",
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
    try:
        op = scan_mysql(
            catalog_params=app_state["catalog_connection"],
            name=name,
            username=username,
            password=password,
            database=database,
            uri=uri,
            port=port,
            scan_type=scan_type,
            incremental=incremental,
            output_format=app_state["output_format"],
            list_all=list_all,
            include_schema_regex=include_schema,
            exclude_schema_regex=exclude_schema,
            include_table_regex=include_table,
            exclude_table_regex=exclude_table,
        )

        typer.echo(message=str_output(op, app_state["output_format"]))
    except NoMatchesError:
        typer.echo(message=NoMatchesError.message)
        typer.Exit(1)


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
    incremental: bool = typer.Option(
        True, help="Scan columns updated or created since last run",
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
    try:
        op = scan_redshift(
            catalog_params=app_state["catalog_connection"],
            name=name,
            username=username,
            password=password,
            database=database,
            uri=uri,
            port=port,
            scan_type=scan_type,
            incremental=incremental,
            output_format=app_state["output_format"],
            list_all=list_all,
            include_schema_regex=include_schema,
            exclude_schema_regex=exclude_schema,
            include_table_regex=include_table,
            exclude_table_regex=exclude_table,
        )

        typer.echo(message=str_output(op, app_state["output_format"]))
    except NoMatchesError:
        typer.echo(message=NoMatchesError.message)
        typer.Exit(1)


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
    incremental: bool = typer.Option(
        True, help="Scan columns updated or created since last run",
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
    try:
        op = scan_snowflake(
            catalog_params=app_state["catalog_connection"],
            name=name,
            username=username,
            password=password,
            database=database,
            account=account,
            warehouse=warehouse,
            role=role,
            scan_type=scan_type,
            incremental=incremental,
            output_format=app_state["output_format"],
            list_all=list_all,
            include_schema_regex=include_schema,
            exclude_schema_regex=exclude_schema,
            include_table_regex=include_table,
            exclude_table_regex=exclude_table,
        )

        typer.echo(message=str_output(op, app_state["output_format"]))
    except NoMatchesError:
        typer.echo(message=NoMatchesError.message)
        typer.Exit(1)


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
    incremental: bool = typer.Option(
        True, help="Scan columns updated or created since last run",
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
    try:
        op = scan_athena(
            catalog_params=app_state["catalog_connection"],
            name=name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
            s3_staging_dir=s3_staging_dir,
            scan_type=scan_type,
            incremental=incremental,
            output_format=app_state["output_format"],
            list_all=list_all,
            include_schema_regex=include_schema,
            exclude_schema_regex=exclude_schema,
            include_table_regex=include_table,
            exclude_table_regex=exclude_table,
        )

        typer.echo(message=str_output(op, app_state["output_format"]))
    except NoMatchesError:
        typer.echo(message=NoMatchesError.message)
        typer.Exit(1)
