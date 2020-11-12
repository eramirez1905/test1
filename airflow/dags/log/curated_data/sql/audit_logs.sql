-- audit logs hurrier

CREATE TEMP FUNCTION string2array(json STRING)
RETURNS ARRAY<STRING>
LANGUAGE js AS """
  if (JSON.parse(json) == null)
      return [];
  else
      return JSON.parse(json);
""";

CREATE TEMPORARY FUNCTION parse_cancel_order_hurrier(new_data STRING, old_data STRING) AS
(
  STRUCT(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.reason'), JSON_EXTRACT_SCALAR(old_data, '$.reason')) AS reason
  )
);

CREATE TEMPORARY FUNCTION parse_update_order_hurrier(new_data STRING, old_data STRING) AS
(
  STRUCT(CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.id'), JSON_EXTRACT_SCALAR(old_data, '$.id')) AS INT64) AS order_id
    , COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.status'), JSON_EXTRACT_SCALAR(old_data, '$.status')) AS status
  )
);

CREATE TEMPORARY FUNCTION parse_replace_delivery_hurrier(new_data STRING, old_data STRING, rider_id INT64) AS
(
  STRUCT(CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.order_id'), JSON_EXTRACT_SCALAR(old_data, '$.order_id')) AS INT64) AS order_id
    , rider_id
  )
);

CREATE TEMPORARY FUNCTION parse_update_delivery_status_hurrier(new_data STRING, old_data STRING) AS
(
  STRUCT(CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.order_id'), JSON_EXTRACT_SCALAR(old_data, '$.order_id')) AS INT64) AS order_id
    , COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.status'), JSON_EXTRACT_SCALAR(old_data, '$.status')) AS status
  )
);

CREATE TEMPORARY FUNCTION parse_manual_dispatch_hurrier(new_data STRING, old_data STRING, rider_id INT64) AS
(
  STRUCT(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.status'), JSON_EXTRACT_SCALAR(old_data, '$.status')) AS status
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.order_id'), JSON_EXTRACT_SCALAR(old_data, '$.order_id')) AS INT64) AS order_id
    , rider_id
  )
);

CREATE TEMPORARY FUNCTION parse_manual_undispatch_hurrier(new_data STRING, old_data STRING, rider_id INT64) AS
(
  STRUCT(COALESCE(JSON_EXTRACT_SCALAR(old_data, '$.status'), JSON_EXTRACT_SCALAR(new_data, '$.status')) AS old_status
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(old_data, '$.order_id'), JSON_EXTRACT_SCALAR(new_data, '$.order_id')) AS INT64) AS order_id
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(old_data, '$.courier_manual_dispatched'), JSON_EXTRACT_SCALAR(new_data, '$.courier_manual_dispatched')) AS BOOL) AS old_courier_manual_dispatched
    , rider_id AS old_rider_id
  )
);

CREATE TEMPORARY FUNCTION parse_courier_updated_hurrier(new_data STRING, old_data STRING) AS
(
  STRUCT(COALESCE(JSON_EXTRACT_SCALAR(old_data, '$.email'), JSON_EXTRACT_SCALAR(new_data, '$.email')) AS old_email
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(old_data, '$.name'), JSON_EXTRACT_SCALAR(new_data, '$.name')) AS STRING) AS old_name
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(old_data, '$.user_id'), JSON_EXTRACT_SCALAR(new_data, '$.user_id')) AS INT64) AS old_user_id
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(old_data, '$.active'), JSON_EXTRACT_SCALAR(new_data, '$.active')) AS BOOL) AS old_is_active
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(old_data, '$.enabled'), JSON_EXTRACT_SCALAR(new_data, '$.enabled')) AS BOOL) AS old_is_enabled
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(old_data, '$.has_bag'), JSON_EXTRACT_SCALAR(new_data, '$.has_bag')) AS BOOL) AS old_has_bag
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(old_data, '$.vehicle_id'), JSON_EXTRACT_SCALAR(new_data, '$.vehicle_id')) AS INT64) AS old_vehicle_id
   )
);

CREATE TEMPORARY FUNCTION parse_create_shift_hurrier(new_data STRING, old_data STRING, rider_id INT64) AS
(
  STRUCT(SAFE_CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, "$.scheduler_token"), JSON_EXTRACT_SCALAR(old_data, "$.scheduler_token")) AS INT64) AS shift_id
    , rider_id
  )
);

CREATE TEMPORARY FUNCTION parse_update_shift_hurrier(new_data STRING, old_data STRING) AS
(
  STRUCT(SAFE_CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, "$.metadata.shiftplan_id"), JSON_EXTRACT_SCALAR(old_data, "$.metadata.shiftplan_id")) AS INT64) AS shift_id
  )
);

CREATE TEMPORARY FUNCTION parse_deactivate_shift_hurrier(new_data STRING, old_data STRING) AS
(
  STRUCT(SAFE_CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, "$.scheduler_token"), JSON_EXTRACT_SCALAR(old_data, "$.scheduler_token")) AS INT64) AS shift_id
  )
);

CREATE TEMPORARY FUNCTION parse_extend_shift_hurrier(new_data STRING, old_data STRING) AS
(
  STRUCT(SAFE_CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, "$.metadata.shiftplan_id"), JSON_EXTRACT_SCALAR(old_data, "$.metadata.shiftplan_id")) AS INT64)  AS shift_id
  )
);

CREATE TEMPORARY FUNCTION parse_finish_ongoing_shift_hurrier(new_data STRING, old_data STRING, rider_id INT64) AS
(
  STRUCT(SAFE_CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, "$.metadata.shiftplan_id"), JSON_EXTRACT_SCALAR(old_data, "$.metadata.shiftplan_id")) AS INT64) AS shift_id
    , rider_id
  )
);

CREATE TEMPORARY FUNCTION parse_force_connect_hurrier(new_data STRING, old_data STRING, rider_id INT64) AS
(
  STRUCT(rider_id
  )
);

CREATE TEMPORARY FUNCTION parse_courier_break_hurrier(new_data STRING, old_data STRING, rider_id INT64) AS
(
  STRUCT(rider_id
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.current_lat'), JSON_EXTRACT_SCALAR(old_data, '$.current_lat')) AS FLOAT64) AS courier_lat
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.current_long'), JSON_EXTRACT_SCALAR(old_data, '$.current_long')) AS FLOAT64) AS courier_long
  )
);

CREATE TEMPORARY FUNCTION parse_courier_working_hurrier(new_data STRING, old_data STRING, rider_id INT64) AS
(
  STRUCT(rider_id
  )
);

CREATE TEMPORARY FUNCTION parse_courier_temp_not_working_hurrier(new_data STRING, old_data STRING, rider_id INT64) AS
(
  STRUCT(rider_id
  )
);

CREATE TEMPORARY FUNCTION parse_dismiss_issue_hurrier(new_data STRING, old_data STRING, rider_id INT64) AS
(
  STRUCT(rider_id
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.id'), JSON_EXTRACT_SCALAR(old_data, '$.id')) AS INT64) AS issue_id
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.delivery_id'), JSON_EXTRACT_SCALAR(old_data, '$.delivery_id')) AS INT64) AS delivery_id
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.show_on_watchlist'), JSON_EXTRACT_SCALAR(old_data, '$.show_on_watchlist')) AS BOOL) AS show_on_watchlist
  )
);

CREATE TEMPORARY FUNCTION parse_resolve_issue_hurrier(new_data STRING, old_data STRING) AS
(
  STRUCT(CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.id'), JSON_EXTRACT_SCALAR(old_data, '$.id')) AS INT64) AS issue_id
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.external_id'), JSON_EXTRACT_SCALAR(old_data, '$.external_id')) AS STRING) AS external_id
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.notes'), JSON_EXTRACT_SCALAR(old_data, '$.notes')) AS STRING) AS issue_notes
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.show_on_watchlist'), JSON_EXTRACT_SCALAR(old_data, '$.show_on_watchlist')) AS BOOL) AS show_on_watchlist
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.category'), JSON_EXTRACT_SCALAR(old_data, '$.category')) AS STRING) AS category
  )
);

CREATE TEMPORARY FUNCTION parse_update_courier_route_hurrier(new_data STRING, old_data STRING, rider_id INT64) AS
(
  STRUCT(rider_id
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.event_type'), JSON_EXTRACT_SCALAR(old_data, '$.event_type')) AS STRING) AS event_type
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.delivery_id'), JSON_EXTRACT_SCALAR(old_data, '$.delivery_id')) AS INT64) AS delivery_id
  )
);

CREATE TEMPORARY FUNCTION parse_change_dropoff_address_hurrier(metadata STRING, new_data STRING, old_data STRING) AS
(
  STRUCT(CAST(JSON_EXTRACT_SCALAR(metadata, '$.distance_difference') AS STRING) AS distance_difference
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.latitude'), JSON_EXTRACT_SCALAR(old_data, '$.latitude')) AS FLOAT64) AS latitude
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.longitude'), JSON_EXTRACT_SCALAR(old_data, '$.longitude')) AS FLOAT64) AS longitude
  )
);

CREATE TEMPORARY FUNCTION parse_update_order_dispatchers_notes_hurrier(new_data STRING, old_data STRING, note STRING) AS
(
  STRUCT(CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.status'), JSON_EXTRACT_SCALAR(old_data, '$.status')) AS STRING) AS status
    , note
  )
);

-- audit logs rooster

CREATE TEMPORARY FUNCTION parse_delete_unassigned_shift_rooster(new_data STRING, old_data STRING) AS
(
  STRUCT(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.tag'), JSON_EXTRACT_SCALAR(old_data, '$.tag')) AS tag
    , COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.state'), JSON_EXTRACT_SCALAR(old_data, '$.state')) AS state
    , SAFE_CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.created_by'), JSON_EXTRACT_SCALAR(old_data, '$.created_by')) AS INT64) AS created_by
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.slots'), JSON_EXTRACT_SCALAR(old_data, '$.slots')) AS INT64) AS slots
  )
);

CREATE TEMPORARY FUNCTION parse_create_unassigned_shift_rooster(new_data STRING, old_data STRING) AS
(
  STRUCT(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.tag'), JSON_EXTRACT_SCALAR(old_data, '$.tag')) AS tag
    , COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.state'), JSON_EXTRACT_SCALAR(old_data, '$.state')) AS state
    , SAFE_CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.created_by'), JSON_EXTRACT_SCALAR(old_data, '$.created_by')) AS INT64) AS created_by
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.slots'), JSON_EXTRACT_SCALAR(old_data, '$.slots')) AS INT64) AS slots
  )
);

CREATE TEMPORARY FUNCTION parse_update_employee_starting_points_rooster(new_data STRING, old_data STRING) AS
(
  STRUCT(CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.employee_id'), JSON_EXTRACT_SCALAR(old_data, '$.employee_id')) AS INT64) AS rider_id
    , COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.starting_points'), JSON_EXTRACT_SCALAR(old_data, '$.starting_points')) AS starting_points
  )
);

CREATE TEMPORARY FUNCTION parse_update_shift_rooster(new_data STRING, old_data STRING) AS
(
  STRUCT(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.tag'), JSON_EXTRACT_SCALAR(old_data, '$.tag')) AS tag
    , COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.state'), JSON_EXTRACT_SCALAR(old_data, '$.state')) AS state
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.starting_point_id'), JSON_EXTRACT_SCALAR(old_data, '$.starting_point_id')) AS INT64)  AS starting_point_id
    , SAFE_CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.created_by'), JSON_EXTRACT_SCALAR(old_data, '$.created_by')) AS INT64) AS created_by
    , SAFE_CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.updated_by'), JSON_EXTRACT_SCALAR(old_data, '$.updated_by')) AS INT64) AS updated_by
  )
);

CREATE TEMPORARY FUNCTION parse_publish_shift_rooster(new_data STRING, old_data STRING) AS
(
  STRUCT(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.tag'), JSON_EXTRACT_SCALAR(old_data, '$.tag')) AS tag
    , COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.state'), JSON_EXTRACT_SCALAR(old_data, '$.state')) AS state
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.starting_point_id'), JSON_EXTRACT_SCALAR(old_data, '$.starting_point_id')) AS INT64)  AS starting_point_id
    , SAFE_CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.created_by'), JSON_EXTRACT_SCALAR(old_data, '$.created_by')) AS INT64) AS created_by
    , SAFE_CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.updated_by'), JSON_EXTRACT_SCALAR(old_data, '$.updated_by')) AS INT64) AS updated_by
  )
);

CREATE TEMPORARY FUNCTION parse_delete_shift_rooster(new_data STRING, old_data STRING) AS
(
  STRUCT(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.id'), JSON_EXTRACT_SCALAR(old_data, '$.id')) AS shift_id
    , COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.tag'), JSON_EXTRACT_SCALAR(old_data, '$.tag')) AS tag
    , COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.state'), JSON_EXTRACT_SCALAR(old_data, '$.state')) AS state
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.starting_point_id'), JSON_EXTRACT_SCALAR(old_data, '$.starting_point_id')) AS INT64)  AS starting_point_id
    , SAFE_CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.created_by'), JSON_EXTRACT_SCALAR(old_data, '$.created_by')) AS INT64) AS created_by
    , SAFE_CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.updated_by'), JSON_EXTRACT_SCALAR(old_data, '$.updated_by')) AS INT64) AS updated_by
    , CAST(REGEXP_EXTRACT(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.start_at'), JSON_EXTRACT_SCALAR(old_data, '$.start_at')), r"^\d+-\d+-\d+T\d+:\d+:\d+[+]\d+:\d+") AS TIMESTAMP) AS start_at
    , CAST(REGEXP_EXTRACT(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.end_at'), JSON_EXTRACT_SCALAR(old_data, '$.end_at')), r"^\d+-\d+-\d+T\d+:\d+:\d+[+]\d+:\d+") AS TIMESTAMP) AS end_at
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.employee_id'), JSON_EXTRACT_SCALAR(old_data, '$.employee_id')) AS INT64) AS rider_id
    , COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.days_of_week'), JSON_EXTRACT_SCALAR(old_data, '$.days_of_week')) AS day_of_week
    , COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.unassigned_shift_id'), JSON_EXTRACT_SCALAR(old_data, '$.unassigned_shift_id')) AS unassigned_shift_id
    , COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.schedule_time_zone'), JSON_EXTRACT_SCALAR(old_data, '$.schedule_time_zone')) AS timezone
  )
);

CREATE TEMPORARY FUNCTION parse_update_unassigned_shift_rooster(new_data STRING, old_data STRING) AS
(
  STRUCT(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.tag'), JSON_EXTRACT_SCALAR(old_data, '$.tag')) AS tag
    , COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.state'), JSON_EXTRACT_SCALAR(old_data, '$.state')) AS state
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.slots'), JSON_EXTRACT_SCALAR(old_data, '$.slots')) AS INT64) AS slots
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.starting_point_id'), JSON_EXTRACT_SCALAR(old_data, '$.starting_point_id')) AS INT64)  AS starting_point_id
    , SAFE_CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.created_by'), JSON_EXTRACT_SCALAR(old_data, '$.created_by')) AS INT64) AS created_by
    , SAFE_CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.updated_by'), JSON_EXTRACT_SCALAR(old_data, '$.updated_by')) AS INT64) AS updated_by
  )
);

CREATE TEMPORARY FUNCTION parse_create_shift_rooster_rooster(new_data STRING, old_data STRING) AS
(
  STRUCT(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.id'), JSON_EXTRACT_SCALAR(old_data, '$.id')) AS shift_id
    , COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.tag'), JSON_EXTRACT_SCALAR(old_data, '$.tag')) AS tag
    , COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.state'), JSON_EXTRACT_SCALAR(old_data, '$.state')) AS state
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.starting_point_id'), JSON_EXTRACT_SCALAR(old_data, '$.starting_point_id')) AS INT64)  AS starting_point_id
    , SAFE_CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.created_by'), JSON_EXTRACT_SCALAR(old_data, '$.created_by')) AS INT64) AS created_by
    , SAFE_CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.updated_by'), JSON_EXTRACT_SCALAR(old_data, '$.updated_by')) AS INT64) AS updated_by
    , CAST(REGEXP_EXTRACT(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.start_at'), JSON_EXTRACT_SCALAR(old_data, '$.start_at')), r"^\d+-\d+-\d+T\d+:\d+:\d+[+]\d+:\d+") AS TIMESTAMP) AS start_at
    , CAST(REGEXP_EXTRACT(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.end_at'), JSON_EXTRACT_SCALAR(old_data, '$.end_at')), r"^\d+-\d+-\d+T\d+:\d+:\d+[+]\d+:\d+") AS TIMESTAMP) AS end_at
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.employee_id'), JSON_EXTRACT_SCALAR(old_data, '$.employee_id')) AS INT64) AS rider_id
    , COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.days_of_week'), JSON_EXTRACT_SCALAR(old_data, '$.days_of_week')) AS day_of_week
    , COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.unassigned_shift_id'), JSON_EXTRACT_SCALAR(old_data, '$.unassigned_shift_id')) AS unassigned_shift_id
    , COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.schedule_time_zone'), JSON_EXTRACT_SCALAR(old_data, '$.schedule_time_zone')) AS timezone
   )
);

CREATE TEMPORARY FUNCTION parse_publish_unassigned_shift_rooster(new_data STRING, old_data STRING) AS
(
  STRUCT(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.id'), JSON_EXTRACT_SCALAR(old_data, '$.id')) AS unassigned_shift_id
    , COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.tag'), JSON_EXTRACT_SCALAR(old_data, '$.tag')) AS tag
    , COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.state'), JSON_EXTRACT_SCALAR(old_data, '$.state')) AS state
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.slots'), JSON_EXTRACT_SCALAR(old_data, '$.slots')) AS INT64) AS slots
    , CAST(REGEXP_EXTRACT(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.start_at'), JSON_EXTRACT_SCALAR(old_data, '$.start_at')), r"^\d+-\d+-\d+T\d+:\d+:\d+[+]\d+:\d+") AS TIMESTAMP) AS start_at
    , CAST(REGEXP_EXTRACT(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.end_at'), JSON_EXTRACT_SCALAR(old_data, '$.end_at')), r"^\d+-\d+-\d+T\d+:\d+:\d+[+]\d+:\d+") AS TIMESTAMP) AS end_at
    , CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.starting_point_id'), JSON_EXTRACT_SCALAR(old_data, '$.starting_point_id')) AS INT64)  AS starting_point_id
    , SAFE_CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.created_by'), JSON_EXTRACT_SCALAR(old_data, '$.created_by')) AS INT64) AS created_by
    , SAFE_CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.updated_by'), JSON_EXTRACT_SCALAR(old_data, '$.updated_by')) AS INT64) AS updated_by
  )
);

CREATE TEMPORARY FUNCTION parse_create_swap_rooster_rooster(new_data STRING, old_data STRING) AS
(
  STRUCT (COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.id'), JSON_EXTRACT_SCALAR(old_data, '$.id')) AS swap_id
    , COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.shift_id'), JSON_EXTRACT_SCALAR(old_data, '$.shift_id')) AS shift_id
    , CAST(REGEXP_EXTRACT(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.accepted_at'), JSON_EXTRACT_SCALAR(old_data, '$.accepted_at')), r"^\d+-\d+-\d+T\d+:\d+:\d+[+]\d+:\d+") AS TIMESTAMP) AS accepted_at
    , SAFE_CAST(COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.accepted_by'), JSON_EXTRACT_SCALAR(old_data, '$.accepted_by')) AS INT64) AS accepted_by
  )
);

CREATE TEMPORARY FUNCTION parse_create_campaign(new_data STRING, old_data STRING) AS
(
  STRUCT(COALESCE(
      JSON_EXTRACT_SCALAR(new_data, '$.data.id'),
      JSON_EXTRACT_SCALAR(old_data, '$.data.id')
    ) AS campaign_id
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.name'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.name')
    ) AS campaign_name
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.is_active'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.is_active')
    ) AS is_active
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.shape'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.shape')
    ) AS shape
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.backend_settings.fee'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.backend_settings.fee')
    ) AS fee
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.backend_settings.type'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.backend_settings.type')
    ) AS fee_type
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.backend_settings.minimum_value'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.backend_settings.minimum_value')
    ) AS minimum_value
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.backend_settings.delivery_time'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.backend_settings.delivery_time')
    ) AS delivery_time
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.filters'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.filters')
    ) AS filters
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.starts_at'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.starts_at')
    ) AS starts_at
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.ends_at'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.ends_at')
    ) AS ends_at
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.drive_times'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.drive_times')
    ) AS drive_times
  )
);

CREATE TEMPORARY FUNCTION parse_create_delivery_area(action STRING, new_data STRING, old_data STRING) AS
(
  STRUCT(COALESCE(
      JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.restaurant_id'),
      JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.restaurant_id')
    ) AS vendor_code
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.drive_times'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.drive_times')
    ) AS drive_times
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.shape'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.shape')
    ) AS shape
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.backend_settings.fee'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.backend_settings.fee')
    ) AS fee
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.backend_settings.type'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.backend_settings.type')
    ) AS fee_type
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.backend_settings.minimum_value'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.backend_settings.minimum_value')
    ) AS minimum_value
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.backend_settings.delivery_time'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.backend_settings.delivery_time')
    ) AS delivery_time
    , SAFE_CAST(COALESCE(
        JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.created_at'),
        JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.created_at')) AS TIMESTAMP
      ) AS created_at
    , SAFE_CAST(COALESCE(
        JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.updated_at'),
        JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.updated_at')) AS TIMESTAMP
      ) AS updated_at
    , COALESCE(
        JSON_EXTRACT_SCALAR(new_data, '$.data.id'),
        JSON_EXTRACT_SCALAR(old_data, '$.data.id')
      ) AS id
    , CASE
        WHEN action = 'delivery_area_create'
          THEN 'created'
        WHEN action = 'delivery_area_modify_or_create' AND old_data IS NULL
          THEN 'created'
        WHEN action = 'delivery_area_modify_or_create' AND old_data IS NOT NULL
          THEN 'updated'
        WHEN action = 'delivery_area_delete'
          THEN 'deleted'
        ELSE NULL
      END AS operation_type
  )
);

CREATE TEMPORARY FUNCTION parse_create_events(new_data STRING, old_data STRING) AS
(
  STRUCT(COALESCE(
      JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.id'),
      JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.id')
    ) AS event_id
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.title'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.title')
    ) AS event_title
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.action'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.action')
    ) AS event_action
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.is_active'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.is_active')
    ) AS is_active
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.tags'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.tags')
    ) AS tags
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.shape'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.shape')
    ) AS shape
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.zone_id'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.zone_id')
    ) AS zone_id
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.shape_sync'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.shape_sync')
    ) AS shape_sync
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.value'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.value')
    ) AS shrink_value
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.activation_threshold'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.activation_threshold')
    ) AS activation_threshold
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.deactivation_threshold'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.deactivation_threshold')
    ) AS deactivation_threshold
  )
);

CREATE TEMPORARY FUNCTION parse_create_city(new_data STRING, old_data STRING) AS
(
  STRUCT(COALESCE(
      JSON_EXTRACT_SCALAR(new_data, '$.data.id'),
      JSON_EXTRACT_SCALAR(old_data, '$.data.id')
    ) AS city_id
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.name'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.name')
    ) AS city_name
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.is_active'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.is_active')
    ) AS is_active
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.order_value_limit'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.order_value_limit')
    ) AS order_value_limit
  )
);

CREATE TEMPORARY FUNCTION parse_create_staring_point(new_data STRING, old_data STRING) AS
(
  STRUCT(COALESCE(
      JSON_EXTRACT_SCALAR(new_data, '$.data.id'),
      JSON_EXTRACT_SCALAR(old_data, '$.data.id')
    ) AS starting_point_id
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.zone_id'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.zone_id')
    ) AS zone_id
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.name'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.name')
    ) AS starting_point_name
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.is_active'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.is_active')
    ) AS is_active
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.shape'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.shape')
    ) AS shape
  )
);

CREATE TEMPORARY FUNCTION parse_create_courier_zone(new_data STRING, old_data STRING) AS
(
  STRUCT(COALESCE(
      JSON_EXTRACT_SCALAR(new_data, '$.data.id'),
      JSON_EXTRACT_SCALAR(old_data, '$.data.id')
    ) AS zone_id
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.city_id'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.city_id')
    ) AS city_id
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.fleet_id'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.fleet_id')
    ) AS fleet_id
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.name'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.name')
    ) AS zone_name
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.is_active'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.is_active')
    ) AS is_active
    , COALESCE(
       JSON_EXTRACT_SCALAR(new_data, '$.data.attributes.shape'),
       JSON_EXTRACT_SCALAR(old_data, '$.data.attributes.shape')
    ) AS shape
  )
);

CREATE TEMPORARY FUNCTION parse_optimize_zone(metadata STRING) AS
(
  STRUCT(JSON_EXTRACT_SCALAR(metadata, '$.payload.filters') AS filters
    , JSON_EXTRACT_SCALAR(metadata, '$.payload.zone_map.coordinates') AS shape
    , JSON_EXTRACT_SCALAR(metadata, '$.payload.vehicle_profile') AS vehicle_profile
    , JSON_EXTRACT_SCALAR(metadata, '$.payload.optimizations') AS optimisations
    , JSON_EXTRACT_SCALAR(metadata, '$.payload.cut_on_zone_border') AS cut_on_zone_border
  )
);

--audit logs Arara
CREATE TEMPORARY FUNCTION parse_applicant_move_to_status(new_data STRING, old_data STRING) AS
(
  STRUCT(CAST(JSON_EXTRACT_SCALAR(new_data, '$.current_state') AS STRING) AS new_current_state
    , CAST(JSON_EXTRACT_SCALAR(old_data, '$.current_state') AS STRING) AS old_current_state
  )
);

WITH countries AS (
  SELECT c.country_code
    , c.country_name
    -- taking one timezone per country to use in the final table
    , (SELECT timezone FROM UNNEST (cities) ci LIMIT 1) AS timezone
  FROM `{{ params.project_id }}.cl.countries` c
), riders AS (
  SELECT country_code
    , rider_id
    , courier_id
  FROM `{{ params.project_id }}.cl.riders`
  LEFT JOIN UNNEST(hurrier_courier_ids) courier_id
), al AS (
  SELECT a.*
    , SAFE_CAST(IF(action = 'force_connect'
        , COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.id'), JSON_EXTRACT_SCALAR(old_data, '$.id'))
        , COALESCE(JSON_EXTRACT_SCALAR(new_data, '$.courier_id'), JSON_EXTRACT_SCALAR(old_data, '$.courier_id'))) AS INT64
      ) AS courier_id
  FROM `{{ params.project_id }}.dl.audit_log_audit_log` a
  {%- if not params.backfill %}
  WHERE created_date BETWEEN DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY) AND '{{ next_ds }}'
  {%- endif %}
), dispatcher_notes AS (
  SELECT n.country_code
    , n.user_id
    , n.updated_at
    , n.note
    , o.platform_order_code AS order_code
  FROM `{{ params.project_id }}.dl.hurrier_dispatcher_notes` n
  LEFT JOIN `{{ params.project_id }}.cl.orders` o ON n.country_code = o.country_code
    AND n.order_id = o.order_id
), audit_log AS (
  SELECT a.*
    , string2array(u.roles) as roles
    , r.rider_id
    , n.note
  FROM al a
  LEFT JOIN riders r ON r.country_code = a.country_code
    AND r.courier_id = a.courier_id
  LEFT JOIN dispatcher_notes n ON a.country_code = n.country_code
    AND a.order_code = n.order_code
    AND a.user_id = n.user_id
    AND TIMESTAMP_TRUNC(a.created_at, SECOND) = TIMESTAMP_TRUNC(n.updated_at, SECOND)
  LEFT JOIN `{{ params.project_id }}.dl.iam_users` u ON a.user_id = u.id
    AND a.region = u.region
)
SELECT l.country_code
  , l.action
  , l.created_at
  , l.id AS log_id
  , l.user_id
  , l.email
  , l.roles
  , l.application
  , l.created_date
  , c.country_name
  , c.timezone
  , STRUCT( order_code
      , IF(action = 'cancel_order', parse_cancel_order_hurrier(new_data, old_data), NULL) AS cancel_order
      , IF(action = 'manual_dispatch', parse_manual_dispatch_hurrier(new_data, old_data, rider_id), NULL) AS manual_dispatch
      , IF(action = 'update_order', parse_update_order_hurrier(new_data, old_data), NULL) AS update_order
      , IF(action = 'manual_undispatch', parse_manual_undispatch_hurrier(new_data, old_data, rider_id), NULL) AS manual_undispatch
      , IF(action = 'replace_delivery', parse_replace_delivery_hurrier(new_data, old_data, rider_id), NULL) AS replace_delivery
      , IF(action = 'update_delivery_status', parse_update_delivery_status_hurrier(new_data, old_data), NULL) AS update_delivery_status
      , IF(action = 'create_shift', parse_create_shift_hurrier(new_data, old_data, rider_id), NULL) AS create_shift
      , IF(action = 'deactivate_shift', parse_deactivate_shift_hurrier(new_data, old_data), NULL) AS deactivate_shift
      , IF(action = 'update_shift', parse_update_shift_hurrier(new_data, old_data), NULL) AS update_shift
      , IF(action = 'finish_ongoing_shift', parse_finish_ongoing_shift_hurrier(new_data, old_data, rider_id), NULL)  AS finish_ongoing_shift
      , IF(action = 'force_connect', parse_force_connect_hurrier(new_data, old_data, rider_id), NULL) AS force_connect
      , IF(action = 'courier_break', parse_courier_break_hurrier(new_data, old_data, rider_id), NULL) AS courier_break
      , IF(action = 'courier_temp_not_working', parse_courier_temp_not_working_hurrier(new_data, old_data, rider_id), NULL) AS courier_temp_not_working
      , IF(action = 'dismiss_issue', parse_dismiss_issue_hurrier(new_data, old_data, rider_id), NULL) AS dismiss_issue
      , IF(action = 'resolve_issue', parse_resolve_issue_hurrier(new_data, old_data), NULL) AS resolve_issue
      , IF(action = 'delivery_time_updated', COALESCE(new_data, old_data), NULL) AS delivery_time_updated
      , IF(action = 'send_to_vendor', COALESCE(new_data, old_data), NULL) AS send_to_vendor
      , IF(action = 'update_courier_route', parse_update_courier_route_hurrier(new_data, old_data, rider_id), NULL) AS update_courier_route
      , IF(action = 'courier_working', parse_courier_working_hurrier(new_data, old_data, rider_id), NULL) AS courier_working
      , IF(action = 'extend_shift', parse_extend_shift_hurrier(new_data, old_data), NULL) AS extend_shift
      , IF(action = 'change_dropoff_address', parse_change_dropoff_address_hurrier(metadata, new_data, old_data), NULL) AS change_dropoff_address
      , IF(action = 'update_order_dispatchers_notes', parse_update_order_dispatchers_notes_hurrier(new_data, old_data, note), NULL) AS update_order_dispatchers_notes
      , IF(action = 'courier_updated', parse_courier_updated_hurrier(new_data, old_data), NULL) AS courier_updated
  ) AS hurrier
  , STRUCT(IF(action = 'delete_unassigned_shift', parse_delete_unassigned_shift_rooster(new_data, old_data), NULL) AS delete_unassigned_shift
      , IF(action = 'create_unassigned_shift', parse_create_unassigned_shift_rooster(new_data, old_data), NULL) AS create_unassigned_shift
      , IF(action = 'update_employee_starting_points', parse_update_employee_starting_points_rooster(new_data, old_data), NULL) AS update_employee_starting_points
      , IF(action = 'update_shift', parse_update_shift_rooster(new_data, old_data), NULL) AS update_shift
      , IF(action = 'publish_shift', parse_publish_shift_rooster(new_data, old_data), NULL) AS publish_shift
      , IF(action = 'delete_shift', parse_delete_shift_rooster(new_data, old_data), NULL) AS delete_shift
      , IF(action = 'update_unassigned_shift', parse_update_unassigned_shift_rooster(new_data, old_data), NULL) AS update_unassigned_shift
      , IF(action = 'publish_unassigned_shift', parse_publish_unassigned_shift_rooster(new_data, old_data), NULL) AS publish_unassigned_shift
      , IF(action = 'create_shift', parse_create_shift_rooster_rooster(new_data, old_data), NULL) AS create_shift
      , IF(action = 'create_swap', parse_create_swap_rooster_rooster(new_data, old_data), NULL) AS create_swap
  ) AS rooster
  , STRUCT(IF(action = 'campaign_create', parse_create_campaign(new_data, old_data), NULL) AS campaign_create
      , IF(action = 'campaign_delete', parse_create_campaign(new_data, old_data), NULL) AS campaign_delete
      , IF(action = 'campaign_modify', parse_create_campaign(new_data, old_data), NULL) AS campaign_modify
      , IF(action = 'campaign_replace', parse_create_campaign(new_data, old_data), NULL) AS campaign_replace
      , IF(action = 'delivery_area_create', parse_create_delivery_area(action, new_data, old_data), NULL) AS delivery_area_create
      , IF(action = 'delivery_area_modify_or_create', parse_create_delivery_area(action, new_data, old_data), NULL) AS delivery_area_modify_or_create
      , IF(action = 'delivery_area_delete', parse_create_delivery_area(action, new_data, old_data), NULL) AS delivery_area_delete
      , IF(action = 'event_create', parse_create_events(new_data, old_data), NULL) AS event_create
      , IF(action = 'event_create_replace', parse_create_events(new_data, old_data), NULL) AS event_create_replace
      , IF(action = 'event_delete', parse_create_events(new_data, old_data), NULL) AS event_delete
      , IF(action = 'event_modify', parse_create_events(new_data, old_data), NULL) AS event_modify
      , IF(action = 'city_create', parse_create_city(new_data, old_data), NULL) AS city_create
      , IF(action = 'city_modify', parse_create_city(new_data, old_data), NULL) AS city_modify
      , IF(action = 'city_delete', parse_create_city(new_data, old_data), NULL) AS city_delete
      , IF(action = 'starting_point_create', parse_create_staring_point(new_data, old_data), NULL) AS starting_point_create
      , IF(action = 'staring_point_delete', parse_create_staring_point(new_data, old_data), NULL) AS staring_point_delete
      , IF(action = 'starting_point_modify', parse_create_staring_point(new_data, old_data), NULL) AS starting_point_modify
      , IF(action = 'starting_point_replace', parse_create_staring_point(new_data, old_data), NULL) AS starting_point_replace
      , IF(action = 'starting_point_suggest', parse_create_staring_point(new_data, old_data), NULL) AS starting_point_suggest
      , IF(action = 'zone_create', parse_create_courier_zone(new_data, old_data), NULL) AS zone_create
      , IF(action = 'zone_delete', parse_create_courier_zone(new_data, old_data), NULL) AS zone_delete
      , IF(action = 'zone_replace', parse_create_courier_zone(new_data, old_data), NULL) AS zone_replace
      , IF(action = 'zone_suggest', parse_create_courier_zone(new_data, old_data), NULL) AS zone_suggest
      , IF(action = 'zone_optimize', parse_optimize_zone(metadata), NULL) AS zone_optimize
  ) AS delivery_area
  , STRUCT(IF(action = 'applicant_move_to_status', parse_applicant_move_to_status(new_data, old_data), NULL) AS applicant_move_to_status
  ) AS arara
FROM audit_log l
LEFT JOIN countries c ON l.country_code = c.country_code
WHERE action IN ('cancel_order', 'update_order', 'send_to_vendor', 'manual_dispatch', 'replace_delivery', 'manual_undispatch', 'delivery_time_updated', 'update_courier_route', 'update_delivery_status', 'create_shift',
'deactivate_shift', 'force_connect', 'update_shift', 'courier_break', 'finish_ongoing_shift', 'courier_temp_not_working', 'delete_unassigned_shift', 'create_unassigned_shift', 'publish_unassigned_shift',
'update_employee_starting_points', 'publish_shift', 'delete_shift', 'update_unassigned_shift', 'publish_unassigned_shift', 'dismiss_issue', 'resolve_issue', 'courier_working', 'extend_shift', 'change_dropoff_address',
'update_order_dispatchers_notes', 'courier_updated', 'create_swap', 'delivery_area_create', 'delivery_area_modify_or_create', 'delivery_area_delete', 'campaign_create', 'campaign_delete', 'campaign_modify', 'campaign_replace',
'event_create', 'event_create_replace', 'event_delete', 'event_modify', 'city_create', 'city_modify', 'city_delete', 'starting_point_create', 'staring_point_delete', 'starting_point_modify', 'starting_point_replace',
'starting_point_suggest', 'zone_create', 'zone_delete', 'zone_replace', 'zone_suggest', 'zone_optimize', 'applicant_move_to_status' )
  AND application IN ('dashboard', 'rooster', 'deliveryareas', 'arara')
