SELECT external_id
    , applicant_id
    , rider_country
    , rider_city
    , rider_language
    , email
    , phone_number
    , CAST(hired_date AS STRING) AS hired_date
    , CAST(first_shift_scheduled AS STRING) AS first_shift_scheduled
    , CAST(first_shift_date AS STRING) AS first_shift_date
    , first_name
    , contract_type
    , vehicle_type
    , referred_by_id
    , COALESCE (total_hours, 0) AS total_hours
    , COALESCE (total_orders_delivered, 0) AS total_orders_delivered
    , COALESCE (total_shifts, 0) AS total_shifts
    , CAST(first_cod_date AS STRING) AS first_cod_date
    , CAST(first_payroll_date AS STRING) AS first_payroll_date
    , referral_link
{%- if params.staging_table %}
FROM {{ params.braze_dataset }}.{{ params.table_name }}{{ ts_nodash }}
{% else %}
FROM {{ params.braze_dataset }}.{{ params.table_name }}
{%- endif %}
