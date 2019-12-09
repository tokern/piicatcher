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
                    pii = c.get_pii_types();
                    if pii:
                        field_value[c.get_name()] = list(pii)

                table_info = client.get_table(
                    DatabaseName=s.get_name(),
                    Name=t.get_name()
                )

                logging.debug(table_info)
                parameters = table_info['Table']['Parameters']
                if parameters is None:
                    parameters = {}

                logging.debug("Parameters are :")
                logging.debug(parameters)

                parameters['piicatcher'] = json.dumps(field_value, cls=PiiTypeEncoder, sort_keys=True)

                logging.debug("Updated parameters are :")
                logging.debug(parameters)

                client.update_table(
                    DatabaseName=s.get_name(),
                    TableInput={
                        'Name': t.get_name(),
                        'Parameters': parameters
                    }
                )
