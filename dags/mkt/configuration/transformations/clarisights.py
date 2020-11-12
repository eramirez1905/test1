from collections import OrderedDict
from itertools import product

from airflow import AirflowException

PREFIX_TEMPLATE_PATH = '{{ next_ds }}'


def transform_clarisights_config(source: str, config) -> OrderedDict:
    result = OrderedDict()
    cs_config = config[source]
    for partner_name, partner_conf in cs_config.items():
        tables = []

        try:
            new_config = OrderedDict({
                'app_version': partner_conf['app_version'],
                'project_name': 'clarisights',
                'is_regional': False,
                'source': 'S3',
                'source_format': 'csv',
                'start_date': partner_conf['start_date'],
                'tables': tables,
            })

            for level, dtype in product(partner_conf['levels'], partner_conf['dtypes']):
                pk_columns = [f'{level}_object_id']
                if dtype != 'core':
                    pk_columns.append('date')

                extra_columns = None
                if dtype == 'core':
                    extra_columns = ['CAST(created_at AS DATE) AS created_date']

                prefix_path = f'from_clarisights/delivery_hero/' \
                              f'{partner_conf.get("s3_partner_prefix", partner_name)}/' \
                              f'v{partner_conf["data_version"]}/' \
                              f'{dtype}_{level}/'

                table_def = OrderedDict({
                    'name': f'{dtype}_{level}',
                    'cleanup_enabled': True,
                    'pk_columns': pk_columns,
                    'deduplicate_order_by_column': 'created_at',
                    'filter_import_by_date': False,
                    'partition_by_date': True,
                    'raw_layer': OrderedDict({
                        'updated_at_column': None,
                    }),
                    's3': OrderedDict({
                        'aws_conn': 'cs_s3_conn',
                        'bucket': 'mkt-partners-adwyze',
                        'extra_columns': extra_columns,
                        'prefix': prefix_path + PREFIX_TEMPLATE_PATH,
                    }),
                })

                tables.append(table_def)

                if dtype != 'core':
                    table_def['time_partitioning'] = 'created_at'

                schema_key = f'{dtype}_{level}'
                table_schemas = partner_conf.get('table_schemas', {})
                if schema_key in table_schemas:
                    table_def['s3']['schema_fields'] = table_schemas[schema_key]
        except KeyError as e:
            raise AirflowException(f"Key config {e} missing for partner: '{partner_name}'")

        result[partner_name] = new_config

    return result
