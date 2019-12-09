import logging

from piicatcher.piitypes import PiiTypeEncoder
from piicatcher.store import Store

import boto3
import json


class GlueStore(Store):
    @classmethod
    def save_schemas(cls, explorer):
        schemas = explorer.get_schemas()
        client = boto3.client("glue",
                              region_name=explorer.config.region,
                              aws_access_key_id=explorer.config.access_key,
                              aws_secret_access_key=explorer.config.secret_key)

        logging.debug(client)
        for s in schemas:
            logging.debug("Processing schema %s" % s.get_name())
            for t in s.get_tables():
                logging.debug("Processing table %s" % t.get_name())
                field_value = {}
                for c in t.get_columns():
                    pii = c.get_pii_types()
                    if pii:
                        field_value[c.get_name()] = [str(v) for v in pii]

                table_info = client.get_table(
                    DatabaseName=s.get_name(),
                    Name=t.get_name()
                )

                logging.debug(table_info)

                updated_columns = []
                is_table_updated = False

                for c in table_info['Table']['StorageDescriptor']['Columns']:
                    if c['Name'] in field_value:
                        c['Parameters'] = {
                            'PII': field_value[c['Name']][0]
                        }
                        is_table_updated = True
                    updated_columns.append(c)

                if is_table_updated:
                    updated_params = {}
                    for param in ['Name', 'Description', 'Owner', 'LastAccessTime', 'LastAnalyzedTime', 'Retention',
                                   'StorageDescriptor', 'PartitionKeys', 'ViewOriginalText', 'ViewExpandedText',
                                   'TableType', 'Parameters']:
                        if param in table_info['Table']:
                            updated_params[param] = table_info['Table'][param]

                    updated_params['StorageDescriptor']['Columns'] = updated_columns

                    logging.debug("Updated parameters are :")
                    logging.debug(updated_params)

                    client.update_table(
                        DatabaseName=s.get_name(),
                        TableInput=updated_params
                    )
