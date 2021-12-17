import datetime
import logging
from contextlib import closing
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import dbcat.settings
from dbcat.api import init_db, open_catalog, scan_sources
from dbcat.catalog import Catalog, CatSource
from sqlalchemy.orm.exc import NoResultFound

from piicatcher import detectors
from piicatcher.detectors import DatumDetector, MetadataDetector, detector_registry
from piicatcher.generators import SMALL_TABLE_MAX, column_generator, data_generator
from piicatcher.output import output_dict, output_tabular
from piicatcher.scanner import deep_scan, shallow_scan

LOGGER = logging.getLogger(__name__)


class ScanTypeEnum(str, Enum):
    shallow = "shallow"
    deep = "deep"


class OutputFormat(str, Enum):
    tabular = "tabular"
    json = "json"


def scan_database(
    catalog: Catalog,
    source: CatSource,
    scan_type: ScanTypeEnum = ScanTypeEnum.shallow,
    incremental: bool = True,
    output_format: OutputFormat = OutputFormat.tabular,
    list_all: bool = False,
    include_schema_regex: List[str] = None,
    exclude_schema_regex: List[str] = None,
    include_table_regex: List[str] = None,
    exclude_table_regex: List[str] = None,
    sample_size: int = SMALL_TABLE_MAX,
) -> Union[List[Any], Dict[Any, Any]]:
    message = "Source: {source_name}, scan_type: {scan_type}, include_schema: {include_schema}, \
            exclude_schema: {exclude_schema}, include_table: {include_table}, exclude_schema: {exclude_table}".format(
        source_name=source.name,
        scan_type=str(scan_type),
        include_schema=",".join(include_schema_regex)
        if include_schema_regex is not None
        else "None",
        exclude_schema=",".join(exclude_schema_regex)
        if exclude_schema_regex is not None
        else "None",
        include_table=",".join(include_table_regex)
        if include_table_regex is not None
        else "None",
        exclude_table=",".join(exclude_table_regex)
        if exclude_table_regex is not None
        else "None",
    )

    status_message = "Success"
    exit_code = 0

    with catalog.managed_session:
        last_run: Optional[datetime.datetime] = None
        if incremental:
            last_task = catalog.get_latest_task("piicatcher.{}".format(source.name))
            last_run = last_task.updated_at if last_task is not None else None
            if last_run is not None:
                LOGGER.debug("Last Run at {}", last_run)
            else:
                LOGGER.debug("No last run found")

        try:
            scan_sources(
                catalog=catalog,
                source_names=[source.name],
                include_schema_regex=include_schema_regex,
                exclude_schema_regex=exclude_schema_regex,
                include_table_regex=include_table_regex,
                exclude_table_regex=exclude_table_regex,
            )

            if scan_type == ScanTypeEnum.shallow:
                detector_list = [
                    detector()
                    for detector in detectors.detector_registry.get_all().values()
                    if issubclass(detector, MetadataDetector)
                ]

                shallow_scan(
                    catalog=catalog,
                    detectors=detector_list,
                    work_generator=column_generator(
                        catalog=catalog,
                        source=source,
                        last_run=last_run,
                        exclude_schema_regex_str=exclude_schema_regex,
                        include_schema_regex_str=include_schema_regex,
                        exclude_table_regex_str=exclude_table_regex,
                        include_table_regex_str=include_table_regex,
                    ),
                    generator=column_generator(
                        catalog=catalog,
                        source=source,
                        last_run=last_run,
                        exclude_schema_regex_str=exclude_schema_regex,
                        include_schema_regex_str=include_schema_regex,
                        exclude_table_regex_str=exclude_table_regex,
                        include_table_regex_str=include_table_regex,
                    ),
                )
            else:
                detector_list = [
                    detector()
                    for detector in detectors.detector_registry.get_all().values()
                    if issubclass(detector, DatumDetector)
                ]

                deep_scan(
                    catalog=catalog,
                    detectors=detector_list,
                    work_generator=column_generator(
                        catalog=catalog,
                        source=source,
                        last_run=last_run,
                        exclude_schema_regex_str=exclude_schema_regex,
                        include_schema_regex_str=include_schema_regex,
                        exclude_table_regex_str=exclude_table_regex,
                        include_table_regex_str=include_table_regex,
                    ),
                    generator=data_generator(
                        catalog=catalog,
                        source=source,
                        last_run=last_run,
                        exclude_schema_regex_str=exclude_schema_regex,
                        include_schema_regex_str=include_schema_regex,
                        exclude_table_regex_str=exclude_table_regex,
                        include_table_regex_str=include_table_regex,
                        sample_size=sample_size,
                    ),
                    sample_size=sample_size,
                )

            if output_format == OutputFormat.tabular:
                return output_tabular(
                    catalog=catalog, source=source, list_all=list_all, last_run=last_run
                )
            else:
                return output_dict(
                    catalog=catalog, source=source, list_all=list_all, last_run=last_run
                )
        except Exception as e:
            status_message = str(e)
            exit_code = 1
            raise e
        finally:
            catalog.add_task(
                "piicatcher.{}".format(source.name),
                exit_code,
                "{}.{}".format(message, status_message),
            )


def list_detectors() -> List[str]:
    return list(detector_registry.get_all().keys())


def list_detector_entry_points() -> List[str]:
    return list(detector_registry.get_entry_points().keys())


def scan_sqlite(
    name: str,
    path: Path,
    scan_type: ScanTypeEnum = ScanTypeEnum.shallow,
    incremental: bool = True,
    output_format: OutputFormat = OutputFormat.tabular,
    list_all: bool = False,
    include_schema_regex: List[str] = None,
    exclude_schema_regex: List[str] = None,
    include_table_regex: List[str] = None,
    exclude_table_regex: List[str] = None,
    sample_size: int = SMALL_TABLE_MAX,
) -> Union[List[Any], Dict[Any, Any]]:
    catalog = open_catalog(
        app_dir=dbcat.settings.APP_DIR,
        secret=dbcat.settings.CATALOG_SECRET,
        path=dbcat.settings.CATALOG_PATH,
        host=dbcat.settings.CATALOG_HOST,
        port=dbcat.settings.CATALOG_PORT,
        user=dbcat.settings.CATALOG_USER,
        password=dbcat.settings.CATALOG_PASSWORD,
        database=dbcat.settings.CATALOG_DB,
    )

    with closing(catalog) as catalog:
        init_db(catalog)

        with catalog.managed_session:
            try:
                source = catalog.get_source(name)
            except NoResultFound:
                source = catalog.add_source(
                    name=path.name, uri=str(path), source_type="sqlite"
                )

            return scan_database(
                catalog=catalog,
                source=source,
                scan_type=scan_type,
                incremental=incremental,
                output_format=output_format,
                list_all=list_all,
                include_schema_regex=include_schema_regex,
                exclude_schema_regex=exclude_schema_regex,
                include_table_regex=include_table_regex,
                exclude_table_regex=exclude_table_regex,
                sample_size=sample_size,
            )


def scan_postgresql(
    name: str,
    username: str,
    password: str,
    database: str,
    uri: str,
    port: Optional[int] = None,
    scan_type: ScanTypeEnum = ScanTypeEnum.shallow,
    incremental: bool = True,
    output_format: OutputFormat = OutputFormat.tabular,
    list_all: bool = False,
    include_schema_regex: List[str] = None,
    exclude_schema_regex: List[str] = None,
    include_table_regex: List[str] = None,
    exclude_table_regex: List[str] = None,
    sample_size: int = SMALL_TABLE_MAX,
) -> Union[List[Any], Dict[Any, Any]]:
    catalog = open_catalog(
        app_dir=dbcat.settings.APP_DIR,
        secret=dbcat.settings.CATALOG_SECRET,
        path=dbcat.settings.CATALOG_PATH,
        host=dbcat.settings.CATALOG_HOST,
        port=dbcat.settings.CATALOG_PORT,
        user=dbcat.settings.CATALOG_USER,
        password=dbcat.settings.CATALOG_PASSWORD,
        database=dbcat.settings.CATALOG_DB,
    )

    with closing(catalog) as catalog:
        init_db(catalog)

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

            return scan_database(
                catalog=catalog,
                source=source,
                scan_type=scan_type,
                incremental=incremental,
                output_format=output_format,
                list_all=list_all,
                include_schema_regex=include_schema_regex,
                exclude_schema_regex=exclude_schema_regex,
                include_table_regex=include_table_regex,
                exclude_table_regex=exclude_table_regex,
                sample_size=sample_size,
            )


def scan_mysql(
    name: str,
    username: str,
    password: str,
    database: str,
    uri: str,
    port: Optional[int] = None,
    scan_type: ScanTypeEnum = ScanTypeEnum.shallow,
    incremental: bool = True,
    output_format: OutputFormat = OutputFormat.tabular,
    list_all: bool = False,
    include_schema_regex: List[str] = None,
    exclude_schema_regex: List[str] = None,
    include_table_regex: List[str] = None,
    exclude_table_regex: List[str] = None,
    sample_size: int = SMALL_TABLE_MAX,
) -> Union[List[Any], Dict[Any, Any]]:
    catalog = open_catalog(
        app_dir=dbcat.settings.APP_DIR,
        secret=dbcat.settings.CATALOG_SECRET,
        path=dbcat.settings.CATALOG_PATH,
        host=dbcat.settings.CATALOG_HOST,
        port=dbcat.settings.CATALOG_PORT,
        user=dbcat.settings.CATALOG_USER,
        password=dbcat.settings.CATALOG_PASSWORD,
        database=dbcat.settings.CATALOG_DB,
    )

    with closing(catalog) as catalog:
        init_db(catalog)

        with catalog.managed_session:
            try:
                source = catalog.get_source(name)
                LOGGER.debug(f"{name} source found")
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
                LOGGER.debug(f"{name} source added")

            return scan_database(
                catalog=catalog,
                source=source,
                scan_type=scan_type,
                incremental=incremental,
                output_format=output_format,
                list_all=list_all,
                include_schema_regex=include_schema_regex,
                exclude_schema_regex=exclude_schema_regex,
                include_table_regex=include_table_regex,
                exclude_table_regex=exclude_table_regex,
                sample_size=sample_size,
            )


def scan_redshift(
    name: str,
    username: str,
    password: str,
    database: str,
    uri: str,
    port: Optional[int] = None,
    scan_type: ScanTypeEnum = ScanTypeEnum.shallow,
    incremental: bool = True,
    output_format: OutputFormat = OutputFormat.tabular,
    list_all: bool = False,
    include_schema_regex: List[str] = None,
    exclude_schema_regex: List[str] = None,
    include_table_regex: List[str] = None,
    exclude_table_regex: List[str] = None,
    sample_size: int = SMALL_TABLE_MAX,
) -> Union[List[Any], Dict[Any, Any]]:
    catalog = open_catalog(
        app_dir=dbcat.settings.APP_DIR,
        secret=dbcat.settings.CATALOG_SECRET,
        path=dbcat.settings.CATALOG_PATH,
        host=dbcat.settings.CATALOG_HOST,
        port=dbcat.settings.CATALOG_PORT,
        user=dbcat.settings.CATALOG_USER,
        password=dbcat.settings.CATALOG_PASSWORD,
        database=dbcat.settings.CATALOG_DB,
    )

    with closing(catalog) as catalog:
        init_db(catalog)

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

            return scan_database(
                catalog=catalog,
                source=source,
                scan_type=scan_type,
                incremental=incremental,
                output_format=output_format,
                list_all=list_all,
                include_schema_regex=include_schema_regex,
                exclude_schema_regex=exclude_schema_regex,
                include_table_regex=include_table_regex,
                exclude_table_regex=exclude_table_regex,
                sample_size=sample_size,
            )


def scan_snowflake(
    name: str,
    account: str,
    username: str,
    password: str,
    database: str,
    warehouse: str,
    role: str,
    scan_type: ScanTypeEnum = ScanTypeEnum.shallow,
    incremental: bool = True,
    output_format: OutputFormat = OutputFormat.tabular,
    list_all: bool = False,
    include_schema_regex: List[str] = None,
    exclude_schema_regex: List[str] = None,
    include_table_regex: List[str] = None,
    exclude_table_regex: List[str] = None,
    sample_size: int = SMALL_TABLE_MAX,
) -> Union[List[Any], Dict[Any, Any]]:
    catalog = open_catalog(
        app_dir=dbcat.settings.APP_DIR,
        secret=dbcat.settings.CATALOG_SECRET,
        path=dbcat.settings.CATALOG_PATH,
        host=dbcat.settings.CATALOG_HOST,
        port=dbcat.settings.CATALOG_PORT,
        user=dbcat.settings.CATALOG_USER,
        password=dbcat.settings.CATALOG_PASSWORD,
        database=dbcat.settings.CATALOG_DB,
    )

    with closing(catalog) as catalog:
        init_db(catalog)

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

            return scan_database(
                catalog=catalog,
                source=source,
                scan_type=scan_type,
                incremental=incremental,
                output_format=output_format,
                list_all=list_all,
                include_schema_regex=include_schema_regex,
                exclude_schema_regex=exclude_schema_regex,
                include_table_regex=include_table_regex,
                exclude_table_regex=exclude_table_regex,
                sample_size=sample_size,
            )


def scan_athena(
    name: str,
    region_name: str,
    s3_staging_dir: str,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    scan_type: ScanTypeEnum = ScanTypeEnum.shallow,
    incremental: bool = True,
    output_format: OutputFormat = OutputFormat.tabular,
    list_all: bool = False,
    include_schema_regex: List[str] = None,
    exclude_schema_regex: List[str] = None,
    include_table_regex: List[str] = None,
    exclude_table_regex: List[str] = None,
    sample_size: int = SMALL_TABLE_MAX,
) -> Union[List[Any], Dict[Any, Any]]:
    catalog = open_catalog(
        app_dir=dbcat.settings.APP_DIR,
        secret=dbcat.settings.CATALOG_SECRET,
        path=dbcat.settings.CATALOG_PATH,
        host=dbcat.settings.CATALOG_HOST,
        port=dbcat.settings.CATALOG_PORT,
        user=dbcat.settings.CATALOG_USER,
        password=dbcat.settings.CATALOG_PASSWORD,
        database=dbcat.settings.CATALOG_DB,
    )

    with closing(catalog) as catalog:
        init_db(catalog)

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

            return scan_database(
                catalog=catalog,
                source=source,
                scan_type=scan_type,
                incremental=incremental,
                output_format=output_format,
                list_all=list_all,
                include_schema_regex=include_schema_regex,
                exclude_schema_regex=exclude_schema_regex,
                include_table_regex=include_table_regex,
                exclude_table_regex=exclude_table_regex,
                sample_size=sample_size,
            )
