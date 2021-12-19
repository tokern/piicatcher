import datetime
import logging
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from dbcat.api import scan_sources
from dbcat.catalog import Catalog, CatSource

from piicatcher import detectors
from piicatcher.detectors import DatumDetector, MetadataDetector, detector_registry
from piicatcher.generators import SMALL_TABLE_MAX, column_generator, data_generator
from piicatcher.output import output_dict, output_tabular
from piicatcher.scanner import data_scan, metadata_scan

LOGGER = logging.getLogger(__name__)


class ScanTypeEnum(str, Enum):
    metadata = "metadata"
    data = "data"


class OutputFormat(str, Enum):
    tabular = "tabular"
    json = "json"


def scan_database(
    catalog: Catalog,
    source: CatSource,
    scan_type: ScanTypeEnum = ScanTypeEnum.metadata,
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
        scan_sources(
            catalog=catalog,
            source_names=[source.name],
            include_schema_regex=include_schema_regex,
            exclude_schema_regex=exclude_schema_regex,
            include_table_regex=include_table_regex,
            exclude_table_regex=exclude_table_regex,
        )

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

            if scan_type == ScanTypeEnum.metadata:
                detector_list = [
                    detector()
                    for detector in detectors.detector_registry.get_all().values()
                    if issubclass(detector, MetadataDetector)
                ]

                metadata_scan(
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

                data_scan(
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
