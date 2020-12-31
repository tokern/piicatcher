from argparse import Namespace
from typing import Any, Dict, List, TextIO, Tuple

from piicatcher.explorer.aws import AthenaExplorer
from piicatcher.explorer.databases import (
    MySQLExplorer,
    OracleExplorer,
    PostgreSQLExplorer,
    RedshiftExplorer,
)
from piicatcher.explorer.explorer import Explorer
from piicatcher.explorer.files import IO, Tokenizer
from piicatcher.explorer.sqlite import SqliteExplorer
from piicatcher.scanner import NERScanner, RegexScanner


def scan_file_object(fd: TextIO) -> List[Any]:
    """

    Args:
        fd (file descriptor): A file descriptor open in text mode.

    Returns: A list of PIITypes enum of all the PII types found in the file.

    """
    scanner = IO("api file object", fd)
    context = {
        "tokenizer": Tokenizer(),
        "regex": RegexScanner(),
        "ner": NERScanner(),
    }

    scanner.scan(context)
    return scanner.get_pii_types()


def _scan_db(scanner: Explorer, scan_type: str) -> Dict[Any, Any]:
    if scan_type == "deep":
        scanner.scan()
    elif scan_type == "shallow":
        scanner.shallow_scan()
    else:
        raise AttributeError("Unknown scan type: {}".format(scan_type))

    return scanner.get_dict()


def scan_database(
    connection: Any,
    connection_type: str,
    scan_type: str = "shallow",
    include_schema: Tuple = (),
    exclude_schema: Tuple = (),
    include_table: Tuple = (),
    exclude_table: Tuple = (),
) -> Dict[Any, Any]:
    """
    Args:
        connection (connection): Connection object to a database
        connection_type (str): Database type. Can be one of sqlite, snowflake, athena, redshift, postgres, mysql or oracle
        scan_type (str): Choose deep(scan data) or shallow(scan column names only)
        include_schema (List[str]): Scan only schemas matching any pattern; When this option is not specified, all
                                    non-system schemas in the target database will be scanned. Also, the pattern is
                                    interpreted as a regular expression, so multiple schemas can also be selected
                                    by writing wildcard characters in the pattern.
        exclude_schema (List[str]): List of patterns. Do not scan any schemas matching any pattern. The pattern is
                                    interpreted according to the same rules as include_schema. When both include_schema
                                    and exclude_schema are given, the behavior is to dump just the schemas that
                                    match at least one include_schema pattern but no exclude_schema patterns. If only
                                    exclude_schema is specified, then matching schemas matching are excluded.
        include_table (List[str]):  List of patterns to match table. Similar in behaviour to include_schema.
        exclude_table (List[str]):  List of patterns to exclude matching table. Similar in behaviour to exclude_schema

    Returns:
        dict: A dictionary of schemata, tables and columns

    """

    scanner: Explorer
    if connection_type == "sqlite":
        args = Namespace(
            path=None,
            scan_type=scan_type,
            list_all=None,
            catalog=None,
            include_schema=include_schema,
            exclude_schema=exclude_schema,
            include_table=include_table,
            exclude_table=exclude_table,
        )

        scanner = SqliteExplorer(args)
    elif connection_type == "athena":
        args = Namespace(
            access_key=None,
            secret_key=None,
            staging_dir=None,
            region=None,
            scan_type=scan_type,
            list_all=None,
            include_schema=include_schema,
            exclude_schema=exclude_schema,
            include_table=include_table,
            exclude_table=exclude_table,
            catalog=None,
        )

        scanner = AthenaExplorer(args)
    #    elif connection_type == "snowflake":
    #        args = Namespace(
    #            account=None,
    #            warehouse=None,
    #            database=None,
    #            user=None,
    #            password=None,
    #            authenticator=None,
    #            okta_account_name=None,
    #            oauth_token=None,
    #            oauth_host=None,
    #            scan_type=scan_type,
    #            list_all=None,
    #            catalog=None,
    #            include_schema=include_schema,
    #            exclude_schema=exclude_schema,
    #            include_table=include_table,
    #            exclude_table=exclude_table,
    #        )

    #        scanner = SnowflakeExplorer(args)
    elif (
        connection_type == "mysql"
        or connection_type == "postgres"
        or connection_type == "redshift"
        or connection_type == "oracle"
    ):
        ns = Namespace(
            host=None,
            port=None,
            user=None,
            password=None,
            database=None,
            connection_type=connection_type,
            scan_type=scan_type,
            list_all=None,
            catalog=None,
            include_schema=include_schema,
            exclude_schema=exclude_schema,
            include_table=include_table,
            exclude_table=exclude_table,
        )
        if ns.connection_type == "mysql":
            scanner = MySQLExplorer(ns)
        elif ns.connection_type == "postgres":
            scanner = PostgreSQLExplorer(ns)
        elif ns.connection_type == "redshift":
            scanner = RedshiftExplorer(ns)
        elif ns.connection_type == "oracle":
            scanner = OracleExplorer(ns)
    else:
        raise AttributeError("Unknown connection type: {}".format(connection_type))

    scanner.connection = connection
    return _scan_db(scanner, scan_type)
