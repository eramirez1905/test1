-- When fetching data from Adjust is necessary to have 3 order types:
-- ```reorder, real_acquisition, install```
-- If some of those types are missing it should raise an error and block the DAG process,
-- because this lack of inforamtion won't be able to match users with devices correctly.
SELECT num_of_order_type
FROM (
    SELECT COUNT(DISTINCT order_type) AS num_of_order_type
    FROM {{ adjust.device_schema }}.s2s_total_acq_last_{{ adjust.device_target_period }}_days
    WHERE dwh_company_id = {{ dwh_company_id }}
)
-- specting *3* order types: `reorder, real_acquisition, install`
WHERE num_of_order_type = 3
;
