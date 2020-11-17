from datahub.common.bigquery_task_generator import BigQueryTaskGenerator


def populate_etl(etl: BigQueryTaskGenerator):
    etl.add_operator(table='cities')
    etl.add_operator(table='deliveries', curated_data_dependencies=[
        "stacked_deliveries",
        "riders",
        "orders"
    ])
    etl.add_operator(table='orders', operator_dependencies=[
        'businesses'
    ])
    etl.add_operator(table='schedules')

    etl.add_operator(table='zones')
    etl.add_operator(table='businesses', curated_data_dependencies=[
        'vendors',
    ])
    etl.add_operator(table='audit_log', operator_dependencies=[
        "cities"
    ])
    etl.add_operator(table='shifts',
                     operator_dependencies=[
                         "cities",
                     ],
                     curated_data_dependencies=[
                         "countries",
                     ])
    etl.add_operator(table='issues')
    etl.add_operator(table='riders')
    etl.add_operator(table='city_dispatching_report',
                     ui_color='report',
                     operator_dependencies=[
                         "shifts",
                         "issues",
                         "cities",
                         "audit_log",
                         "orders",
                         "deliveries",
                     ],
                     curated_data_dependencies=[
                         "countries",
                         "_orders_to_zones",
                     ])
    etl.add_operator(table='dispatching_report', ui_color='report', operator_dependencies=[
        "audit_log"
    ])
    etl.add_operator(table='absences')
    etl.add_operator(table='evaluations', curated_data_dependencies=[
        "countries"
    ])
    etl.add_operator(table='rider_payroll_report_orders', ui_color='report', operator_dependencies=[
        'deliveries',
        'orders',
        'riders',
        'cities',
        'zones',
    ])
    etl.add_operator(table='staffing', curated_data_dependencies=[
        "countries",
    ])

    etl.add_operator(table='sr_transitions_dataset', curated_data_dependencies=[
        "_rider_transitions_raw",
        "_rider_working_time",
    ])
    etl.add_operator(table='sr_deliveries_over_interval',
                     operator_dependencies=[
                         "orders",
                         "deliveries",
                     ],
                     curated_data_dependencies=[
                         "countries",
                         "_orders_to_zones",
                     ])
    etl.add_operator(table='sr_orders_over_interval',
                     operator_dependencies=[
                         "orders",
                         "deliveries",
                     ],
                     curated_data_dependencies=[
                         "countries",
                         "_orders_to_zones",
                     ])
    etl.add_operator(table='sr_open_slots_dataset', operator_dependencies=[
        "staffing",
    ])
    etl.add_operator(table='sr_shifts_dataset', operator_dependencies=[
        "shifts",
        "evaluations",
        "absences",
    ])
    etl.add_operator(table='sr_lost_orders', curated_data_dependencies=[
        "countries",
        "_orders_to_zones",
    ])
    etl.add_operator(table='rider_payroll_report_shifts', ui_color='report', operator_dependencies=[
        "schedules",
        "absences",
        "zones",
        "riders",
        "cities",
    ])
    etl.add_operator(table='staffing_report',
                     ui_color='report',
                     operator_dependencies=[
                         "sr_shifts_dataset",
                         "sr_open_slots_dataset",
                         "sr_transitions_dataset",
                         "sr_deliveries_over_interval",
                         "sr_orders_over_interval",
                         "sr_lost_orders",
                     ],
                     curated_data_dependencies=[
                         "countries",
                         "_sr_latest_forecast_dataset"
                     ])
