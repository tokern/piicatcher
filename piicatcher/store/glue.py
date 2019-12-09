from piicatcher.store import Store

import boto3
import json


class GlueStore(Store):
    @classmethod
    def save_schemas(cls, explorer):
        schemas = explorer.get_schemas()
        client = boto3.client("glue",
                              region_name=explorer.config.region,
                              aws_access_key_id=explorer.config.aws_access_key_id,
                              aws_secret_access_key=explorer.config.aws_secret_access_key)

        for s in schemas:
            for t in s.get_tables():
                field_value = {}
                for c in t.get_columns():
                    pii = c.get_pii_types();
                    if not pii.empty():
                        field_value[c.get_name()] = pii;

                client.update_table(
                    DatabaseName=s.get_name(),
                    TableInput={
                        'Name': t.get_name(),
                        'Parameters': {
                            'piicatcher': json.dumps(field_value, sort_keys=True)
                        }
                    }
                )
