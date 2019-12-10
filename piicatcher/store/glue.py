import logging

from piicatcher.store import Store

import boto3


class GlueStore(Store):
    @staticmethod
    def update_column_parameters(column_parameters, pii_table):
        updated_columns = []
        is_table_updated = False

        for c in column_parameters:
            if c['Name'] in pii_table:
                if 'Parameters' not in c or c['Parameters'] is None:
                    c['Parameters'] = {}

                c['Parameters']['PII'] = pii_table[c['Name']][0]
                is_table_updated = True
            updated_columns.append(c)

        return updated_columns, is_table_updated

    @staticmethod
    def get_pii_table(table):
        logging.debug("Processing table %s" % table.get_name())
        field_value = {}
        for c in table.get_columns():
            pii = c.get_pii_types()
            if pii:
                field_value[c.get_name()] = sorted([str(v) for v in pii])
        return field_value

    @staticmethod
    def update_table_params(table_params, column_params):
        updated_params = {}
        for param in ['Name', 'Description', 'Owner', 'LastAccessTime', 'LastAnalyzedTime', 'Retention',
                      'StorageDescriptor', 'PartitionKeys', 'ViewOriginalText', 'ViewExpandedText',
                      'TableType', 'Parameters']:
            if param in table_params:
                updated_params[param] = table_params[param]

        updated_params['StorageDescriptor']['Columns'] = column_params

        logging.debug("Updated parameters are :")
        logging.debug(updated_params)
        return updated_params

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
                field_value = GlueStore.get_pii_table(t)
                table_info = client.get_table(
                    DatabaseName=s.get_name(),
                    Name=t.get_name()
                )

                logging.debug(table_info)

                updated_columns, is_table_updated = GlueStore.update_column_parameters(
                    table_info['Table']['StorageDescriptor']['Columns'], field_value
                )

                if is_table_updated:
                    updated_params = GlueStore.update_table_params(table_info['Table'], updated_columns)
                    client.update_table(
                        DatabaseName=s.get_name(),
                        TableInput=updated_params
                    )
