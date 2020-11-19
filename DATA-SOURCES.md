# How to add new data sources in Airflow

We have an established framework to import different data sources which uses common datahub modules/operators.

[Example](https://github.com/deliveryhero/datahub-airflow/blob/master/dags/log/dwh_import_read_replica.py).

To add a new data source using this setup it mostly means adding a new entry to `config.yaml` file.

All new configurations to import new sources should belong to the key `dwh_merge_layer_databases` in the config file.

The different types of Data sources in our scope are the following:

1. [RDS](#rds)
2. [API](#api)
3. [DynamoDB](#dynamodb)
4. [Big_Query](#big_query)
5. [SQS](#sqs)
6. [RedShift_S3](#redshift_s3)
7. [Firebase](#firebase)
8. [Google_analytics](#google_analytics)
10. [S3](#s3)
11. [Debezium](#debezium-beta) (BETA)

## RDS

This import will mainly create table and views in the following data sets in BigQuery:

1. raw: Tables in this layer contain the raw data as is from the source.
2. dl: Views in this layer contains deduplicated data with the latest record.
3. hl: Views in this layer contain the historical data.

Follow steps 1 to 5 to import a table from a new application:

 1. Check whether the endpoint of the application is already added in spark repository, because all RDS data is imported through spark. You can navigate to spark from [here](https://github.com/deliveryhero/logistics-spark-jobs).
 2. If the table is part of a new application add the application name under the key `dwh_merge_layer_databases`.
    In the below example `hurrier` is the application name.
 3. There are two types of applications `regional` and `non regional` so find out what type of application it is. If 
    it's non-regional add key as `is_regional: false` else as `is_regional: true`.
 4. Add Source of the application, if it's RDS add key and value as `source: RDS`, if it's not RDS use respective values
    like `[API, DynamoDB, Firebase, Google_analytics, Big_Query, SQS, RedShift_S3]`
 5. Add `credentials` which can use in jdbc to fetch data, here the password is a secret key and it should be deployed 
    over helm secrets. Steps to follow are:
        5.1 Navigate to helm repository and run the command:
           `./tools/kube_tool secrets add -p airflow  -e <ENVIRONMENT> --name <KEY_PASSWORD> --value <VALUE_PASSWORD>`
            here `ENVIRONMENT` would be `staging/production`
	Incase of staging also use flag --namespace dwh
        5.2 Add the key over helm chart, refer the [PR](https://github.com/deliveryhero/logistics-infra-helm/pull/4284/)
            to know how to do it.

 6. Make sure the source table has `created_at`, `updated_at` columns and both indexed, otherwise ask the source team to add.    
 7. Add the table name over the key `name` and the name should exactly match with the source table in rds. Once the table is 
    imported in big query, the name would be `<application_name>_<table_name>` Example `hurrier_addresses_zones`.
 8. All other key, value properties under `tables` are optional and add it based on the requirement. Some of the 
    important properties are:
        8.1 pk_columns : columns used for clustering in big query and the combination of these keys would be unique 
            across partitions. The default values of non-regional and regional applications would be `[country_code, id]`
            and `[region, id]`
        8.2 filter_import_by_date: property would use if the import table is just a reference table and less number of 
            records.
           
 If the application exists and the requirement is to import only tables, then follow steps 6 to 8 only.       
 
Example
```yaml
  hurrier:
    is_regional: false
    source: RDS
    credentials:
      username: foodora
      password: JDBC_LOG_PASSWORD
    tables:
      - name: addresses_zones
        pk_columns: [country_code, address_id, zone_id]
```

## API

If the requirement is to import data from api to bigquery, please follow below steps.

1. create dedicated DAG for the api and import data to bigquery (`raw`) through airflow operators.
   To know more about the procedures, refer to the DAGS of Soti / Freshdesk.
2. To create views in dl and hl layer, add corresponding application and table name details in config file.
3. Add source as `API` and mention unique keys as `pk_columns`.

Refer below example of freshdesk data import.
Example
```yaml
  freshdesk:
    is_regional: true
    source: API
    tables:
      - name: reports
        pk_columns: [instance, Ticket_Id]
```

## DynamoDB

If the requirement is to import data from DynamoDB stream to big query, follow below steps.

1. Create a Deserializer to perform necessary transformations and push data to a big query(`raw` layer).
   for more details refer to the example [Deserialiser](https://github.com/deliveryhero/fulfillment-dwh-kinesis-consumer).
2. Deploy the Serializer as AWS lambda, use `Terraform` for the deployment.
3. To create views in dl and hl layer, add corresponding application and table name details in config file.
4. Add source as `API` and mention unique keys as `pk_columns`.
5. Use the attribute `job_config` to skip the data import raw layer.

Example
```yaml
  dms:
    is_regional: true
    source: 'DynamoDB'
    job_config:
      export_read_replica:
        enabled: false
      copy_staging_to_raw_layer:
        enabled: false
    tables:
      - name: push_notification_sent_message
        source: 'DynamoDB'
        sharding_column: region
        cleanup_enabled: false
        pk_columns: [region, deviceUuid]
        raw_layer:
          created_at_column: created_at
          updated_at_column: _ingested_at
        job_config:
          export_read_replica:
            enabled: false
          copy_staging_to_raw_layer:
            enabled: false
```

## SQS

If the requirement is to import SQS data to a big query, follow below steps.

1. Create a Deserializer to perform necessary transformations and push data to a big query(`raw` layer).
   For more details refer the example [Deserialiser](https://github.com/deliveryhero/fulfillment-dwh-sqs-consumer).
2. To create views in dl and hl layer, add corresponding application and table name details in config file.
3. Add source as `SQS` and mention unique keys as `pk_columns`.
4. Use the attribute `raw_layer` to alter the default `created_at` and `updated_at` column names.
5. Use the attribute `job_config` to skip the usual raw layer import.

Example
```yaml
  rps:
    is_regional: true
    source: 'SQS'
    job_config:
      export_read_replica:
        enabled: false
      copy_staging_to_raw_layer:
        enabled: false
    tables:
      - name: monitor
        pk_columns: [region, platformId, platformRestaurantId, event, eventTime]
        cleanup_enabled: false
        partition_by_date: false
        raw_layer:
          created_at_column: created_at
          updated_at_column: _ingested_at
```

## Big_Query

If the requirement is to create a view  in `fulfillment-dwh` from an external table which belongs to different big query
project.
E.g to create a view in `fulfillment-dwh-production.dl.<view_name>` which maps to `datafridge-production.dh_public.<table_name>`

1. To create views in dl and hl layer, add corresponding application and table name details in config file.
2. Add source as `big_query`.
3. Use the attribute `job_config` to skip the usual raw layer import.
4. Add the source table name over the key `source_table`

Example
Use the below configuration to map the datafridge view `datafridge-production.dh_fridge_prod_us_bigquery_dataset_for_logistics.global_entity_ids_logistics` 
as a view in datahub `fulfimment-dwh-production.dl.data_fridge_global_entity_ids`.

```yaml
  data_fridge:
    is_regional: true
    source: 'big_query'
    job_config:
      export_read_replica:
        enabled: false
      copy_staging_to_raw_layer:
        enabled: false
   - name: global_entity_ids
     pk_columns: []
     cleanup_enabled: false
     partition_by_date: false
     authorize_view: true
     time_partitioning: ~
     source_table: datafridge-production.dh_fridge_prod_us_bigquery_dataset_for_logistics.global_entity_ids_logistics
     
```

## RedShift_S3

If the requirement is to import data from RedShift S3 bucket to big query.

1. Add source as `RedShift_S3`.
2. Add S3 bucket name over the key `redshift_s3`.
3. Add the source file format in s3 over the key `source_format`
4. Use the key `redshift_s3` to add S3 table_name / object_name and extra columns need to add in bigquery.

Example
```yaml
  pandora:
    is_regional: true
    source: RedShift_S3
    source_format: parquet
    redshift_s3:
      bucket_name: fulfillment-dwh-staging-eu-pandora-import
    tables:
      - name: traffic
        pk_columns: [dh_source_id, language_code, start_datetime, end_datetime]
        cleanup_enabled: false
        redshift_s3:
          table_name: bl_global_ccn_cc_traffic_dataset
          extra_columns: [iso_date AS created_date, log_country_code AS country_code]
```

## Firebase

If the requirement is to import firebase events to big query, follow the below steps

1. Add source as `firebase`.
2. Use the attribute `job_config` to skip the usual raw layer import.
3. Add the source table name over the key `source_table`

Example
```yaml
  go_droid:
    is_regional: true
    source: 'firebase'
    job_config:
      export_read_replica:
        enabled: false
      copy_staging_to_raw_layer:
        enabled: false
    tables:
      - name: events
        pk_columns: []
        cleanup_enabled: false
        partition_by_date: false
        source_table: rps-own-delivery-live.analytics_156198623.events
```

## Google_analytics

If the requirement is to import google_analytics sections to big query, follow the below steps

1. Add source as `google_analytics`.
2. Use the attribute `job_config` to skip the usual raw layer import.
3. Add the source table name over the key `source_table`

Example
```yaml
  go_win:
    is_regional: true
    source: 'google_analytics'
    job_config:
      export_read_replica:
        enabled: false
      copy_staging_to_raw_layer:
        enabled: false
    tables:
      - name: sessions
        pk_columns: []
        cleanup_enabled: false
        partition_by_date: false
        source_table: dhh---rps-webkick.160669167.ga_sessions

```

## S3

If the requirement is to import data from S3 bucket to BigQuery.

1. Add source as `S3`.
2. Fill required and optional params for this import under `s3` key in table configuration. Available options:

| FIELD | TYPE | REQUIRED | DESCRIPTION |
| :--- | :--- | :--- | :--- |
| bucket | `STRING` | MANDATORY | S3 bucket name |
| prefix | `STRING` | OPTIONAL | prefix used to filter files that are going to be ingested, default: empty string |
| aws_conn_id | `STRING` | OPTIONAL | Airflow Connection ID used to auth with S3, default: `aws_default` |
| extra_columns | `ARRAY<STRING>` | OPTIONAL | List of extra columns to be added in BQ, default: `None` |
| schema_fields | `ARRAY<STRING>` | OPTIONAL | List of schema fields used in case of data being in CSV format, please follow [this](https://cloud.google.com/bigquery/docs/schemas#specifying_a_json_schema_file) format. If not supplied will turn on `autodetect` BigQuery option default: `None` |
| allow_quoted_newlines | `BOOLEAN` | OPTIONAL | Indicates whether to allow quoted data sections that contain newline characters in a CSV file, default: `True` |


Example
```yaml
  apple:
    is_regional: false
    source: S3
    source_format: csv
    tables:
      - name: core_account
        cleanup_enabled: true
        pk_columns: [account_object_id]
        deduplicate_order_by_column: created_at
        filter_import_by_date: false
        partition_by_date: true
        raw_layer:
          updated_at_column: null
        s3:
          aws_conn: cs_s3_conn
          bucket: mkt-partners-adwyze
          extra_columns: [CAST(created_at AS DATE) AS created_date]
          prefix: >
            {%- raw -%}
            from_clarisights/delivery_hero/apple/v3/core_account/{{ next_ds }}
            {%- endraw -%}
          schema_fields:
            - {name: account_object_id, type: INTEGER}
            - {name: source_id, type: INTEGER}
            - {name: created_at, type: TIMESTAMP}
```

If the requirement to import source other than above, create a new configuration with custom changes and do necessary
changes in import jobs.

## Debezium (BETA)

Import the database using Debezium

The pipeline consists of one task that creates a Kafka Connect Debezium source per-database and one task which creates Kafka Connect BigQuery Sink per table.

Example
```yaml
dwh_merge_layer_databases:
  airflow:
    is_regional: true
    source: Debezium
    sharding_column: business_unit
    endpoint: rds.eu.airflow.staging-eu.tf
    port: 3306
    credentials:
      username: foodora
      password: JDBC_LOG_PASSWORD
    tables:
      - name: ab_permission
        time_partitioning: created_date
        cleanup_enabled: false
        sanity_checks:
          created_date_null: true
          duplicate_check: true
        keep_last_import_only: true
        keep_last_import_only_partition_by_pk: true
        raw_layer:
          created_at_column: _ingested_at
          updated_at_column: _ingested_at
```
