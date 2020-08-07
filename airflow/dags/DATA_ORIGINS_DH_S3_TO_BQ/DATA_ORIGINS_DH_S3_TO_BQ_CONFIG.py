schedule_group_1 = "30 10 * * *"
schedule_group_2 = "0 11 * * *"
schedule_group_3 = "45 10 * * *"

monthly_schedule = "0 11 1 * *"

# S3
dmarts_s3_bucket = "peyabi.dh.dmarts"
dmarts_source_dir = "central-dwh/dwh_il_darkstore"
pandora_target_dir = "refined/pandora"
dmarts_gcs_bucket = "data-origins-storage"
dmarts_gcs_target_dir = "dmarts"

dag_bag = {
    'sessions': {
        "testing": False,
        "name": "sessions",
        "active": True,
        "scheduler": schedule_group_1,
        "retries": 5,
        "catchup": False,
        "start_year": 2019,
        "start_month": 9,
        "start_day": 1,
        "date_str": '{{ ds }}',
        "date_id": '{{ execution_date.strftime("%Y%m") }}',
        "model": "sessions",
        "has_dwh_table": True,
        "send_slack": True,
        "date_filter": True,
        "filter_column": "flow_session_start",
        "spark_script": "spark_script.py",
        'update_type': 'FULL',
        'update_prev': True,
        "days_back": 2,
        "country_col": "country",
        "date_id_col":None,
        "types_conversion": None,
        # "partitioning": {
        #     "col_name": "CreatedDate",
        #     "CreatedDate": {
        #         "type": "timestamp",
        #         "level": "day",
        #     }
        # },
        "source_bucket": dmarts_s3_bucket,
        "source_dir": dmarts_source_dir,
        "source_folder": "fct_purchase_orders",
        "target_dir": dmarts_gcs_target_dir,
        "target_folder": "sessions",
        "external_table": "s3.help_center_sessions",
        "external_columns":"""upper(brand) as brand
                        , country
                        , global_entity_id
                        , management_entity
                        , customer_id
                        , flow_session_id
                        , flow_session_start
                        , "language"
                        , platform
                        , region
                        , "source"
                        , version
                        , country_id
                        , date_id""",
        "import_table": "imports.data_help_center_sessions",
        "dwh_table": "public.fact_help_center_sessions",
        "dwh_columns": """brand
                        , country
                        , global_entity_id
                        , management_entity
                        , customer_id
                        , flow_session_id
                        , flow_session_start
                        , "language"
                        , platform
                        , region
                        , "source"
                        , version
                        , country_id
                        , date_id""",
        "dwh_filter": "",
        "dwh_custom_query": "",
        "dwh_date_field": "flow_session_start",
    },
}