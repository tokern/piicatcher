from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from dbcat import Catalog, DbScanner, PGCatalog, SqliteCatalog, init_db
from dbcat.catalog import CatSource
from sqlalchemy.orm.exc import NoResultFound

from piicatcher.generators import column_generator, data_generator
from piicatcher.scanner import deep_scan, shallow_scan


def get_catalog(
    catalog_path: str = None,
    catalog_host: str = None,
    catalog_port: int = None,
    catalog_user: str = None,
    catalog_password: str = None,
    catalog_database: str = None,
) -> Catalog:
    if (
        catalog_host is not None
        and catalog_port is not None
        and catalog_user is not None
        and catalog_password is not None
        and catalog_database is not None
    ):
        catalog = PGCatalog(
            host=catalog_host,
            port=str(catalog_port),
            user=catalog_user,
            password=catalog_password,
            database=catalog_database,
        )
    elif catalog_path is not None:
        catalog = SqliteCatalog(path=str(catalog_path))
    else:
        raise AttributeError(
            "None of Path or Postgres connection parameters are provided"
        )

    init_db(catalog)
    return catalog


class ScanTypeEnum(str, Enum):
    shallow = "shallow"
    deep = "deep"


def scan_database(
    catalog: Catalog,
    source: CatSource,
    scan_type: ScanTypeEnum = ScanTypeEnum.shallow,
    include_schema_regex: List[str] = None,
    exclude_schema_regex: List[str] = None,
    include_table_regex: List[str] = None,
    exclude_table_regex: List[str] = None,
):
    with catalog.managed_session:
        scanner = DbScanner(catalog=catalog, source=source)
        scanner.scan()

        if scan_type == ScanTypeEnum.shallow:
            shallow_scan(
                catalog=catalog,
                generator=column_generator(
                    catalog=catalog,
                    source=source,
                    exclude_schema_regex_str=exclude_schema_regex,
                    include_schema_regex_str=include_schema_regex,
                    exclude_table_regex_str=exclude_table_regex,
                    include_table_regex_str=include_table_regex,
                ),
            )
        else:
            deep_scan(
                catalog=catalog,
                generator=data_generator(
                    catalog=catalog,
                    source=source,
                    exclude_schema_regex_str=exclude_schema_regex,
                    include_schema_regex_str=include_schema_regex,
                    exclude_table_regex_str=exclude_table_regex,
                    include_table_regex_str=include_table_regex,
                ),
            )


def scan_sqlite(
    catalog_params: Dict[str, Any],
    name: str,
    path: Path,
    scan_type: ScanTypeEnum = ScanTypeEnum.shallow,
    include_schema_regex: List[str] = None,
    exclude_schema_regex: List[str] = None,
    include_table_regex: List[str] = None,
    exclude_table_regex: List[str] = None,
):
    catalog = get_catalog(**catalog_params)

    with catalog.managed_session:
        try:
            source = catalog.get_source(name)
        except NoResultFound:
            source = catalog.add_source(
                name=path.name, uri=str(path), source_type="sqlite"
            )

        scan_database(
            catalog=catalog,
            source=source,
            scan_type=scan_type,
            include_schema_regex=include_schema_regex,
            exclude_schema_regex=exclude_schema_regex,
            include_table_regex=include_table_regex,
            exclude_table_regex=exclude_table_regex,
        )


def scan_postgresql(
    catalog_params: Dict[str, Any],
    name: str,
    username: str,
    password: str,
    database: str,
    uri: str,
    port: Optional[int] = None,
    scan_type: ScanTypeEnum = ScanTypeEnum.shallow,
    include_schema_regex: List[str] = None,
    exclude_schema_regex: List[str] = None,
    include_table_regex: List[str] = None,
    exclude_table_regex: List[str] = None,
):
    catalog = get_catalog(**catalog_params)

    with catalog.managed_session:
        try:
            source = catalog.get_source(name)
        except NoResultFound:
            source = catalog.add_source(
                name=name,
                username=username,
                password=password,
                database=database,
                uri=uri,
                port=port,
                source_type="postgresql",
            )

        scan_database(
            catalog=catalog,
            source=source,
            scan_type=scan_type,
            include_schema_regex=include_schema_regex,
            exclude_schema_regex=exclude_schema_regex,
            include_table_regex=include_table_regex,
            exclude_table_regex=exclude_table_regex,
        )


def scan_mysql(
    catalog_params: Dict[str, Any],
    name: str,
    username: str,
    password: str,
    database: str,
    uri: str,
    port: Optional[int] = None,
    scan_type: ScanTypeEnum = ScanTypeEnum.shallow,
    include_schema_regex: List[str] = None,
    exclude_schema_regex: List[str] = None,
    include_table_regex: List[str] = None,
    exclude_table_regex: List[str] = None,
) -> int:
    catalog = get_catalog(**catalog_params)

    with catalog.managed_session:
        try:
            source = catalog.get_source(name)
        except NoResultFound:
            source = catalog.add_source(
                name=name,
                username=username,
                password=password,
                database=database,
                uri=uri,
                port=port,
                source_type="mysql",
            )

        scan_database(
            catalog=catalog,
            source=source,
            scan_type=scan_type,
            include_schema_regex=include_schema_regex,
            exclude_schema_regex=exclude_schema_regex,
            include_table_regex=include_table_regex,
            exclude_table_regex=exclude_table_regex,
        )

        return source.id


def scan_redshift(
    catalog_params: Dict[str, Any],
    name: str,
    username: str,
    password: str,
    database: str,
    uri: str,
    port: Optional[int] = None,
    scan_type: ScanTypeEnum = ScanTypeEnum.shallow,
    include_schema_regex: List[str] = None,
    exclude_schema_regex: List[str] = None,
    include_table_regex: List[str] = None,
    exclude_table_regex: List[str] = None,
):
    catalog = get_catalog(**catalog_params)

    with catalog.managed_session:
        try:
            source = catalog.get_source(name)
        except NoResultFound:
            source = catalog.add_source(
                name=name,
                username=username,
                password=password,
                database=database,
                uri=uri,
                port=port,
                source_type="redshift",
            )

        scan_database(
            catalog=catalog,
            source=source,
            scan_type=scan_type,
            include_schema_regex=include_schema_regex,
            exclude_schema_regex=exclude_schema_regex,
            include_table_regex=include_table_regex,
            exclude_table_regex=exclude_table_regex,
        )


def scan_snowflake(
    catalog_params: Dict[str, Any],
    name: str,
    account: str,
    username: str,
    password: str,
    database: str,
    warehouse: str,
    role: str,
    scan_type: ScanTypeEnum = ScanTypeEnum.shallow,
    include_schema_regex: List[str] = None,
    exclude_schema_regex: List[str] = None,
    include_table_regex: List[str] = None,
    exclude_table_regex: List[str] = None,
):
    catalog = get_catalog(**catalog_params)

    with catalog.managed_session:
        try:
            source = catalog.get_source(name)
        except NoResultFound:
            source = catalog.add_source(
                name=name,
                username=username,
                password=password,
                database=database,
                account=account,
                warehouse=warehouse,
                role=role,
                source_type="snowflake",
            )

        scan_database(
            catalog=catalog,
            source=source,
            scan_type=scan_type,
            include_schema_regex=include_schema_regex,
            exclude_schema_regex=exclude_schema_regex,
            include_table_regex=include_table_regex,
            exclude_table_regex=exclude_table_regex,
        )


def scan_athena(
    catalog_params: Dict[str, Any],
    name: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    region_name: str,
    s3_staging_dir: str,
    scan_type: ScanTypeEnum = ScanTypeEnum.shallow,
    include_schema_regex: List[str] = None,
    exclude_schema_regex: List[str] = None,
    include_table_regex: List[str] = None,
    exclude_table_regex: List[str] = None,
):
    catalog = get_catalog(**catalog_params)

    with catalog.managed_session:
        try:
            source = catalog.get_source(name)
        except NoResultFound:
            source = catalog.add_source(
                name=name,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name,
                s3_staging_dir=s3_staging_dir,
                source_type="athena",
            )

        scan_database(
            catalog=catalog,
            source=source,
            scan_type=scan_type,
            include_schema_regex=include_schema_regex,
            exclude_schema_regex=exclude_schema_regex,
            include_table_regex=include_table_regex,
            exclude_table_regex=exclude_table_regex,
        )
