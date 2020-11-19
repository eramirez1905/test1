-- Audience Device_id - Customer_id Mapping

-- Get from Adjust all the Acquisition events for PeYa (no country distinction)
DROP TABLE IF EXISTS adjust_events;
CREATE TEMP TABLE adjust_events AS
SELECT DISTINCT
    dwh_company_id,
    transaction_id,
    user_id as customer_id,
    COALESCE(NULLIF(gps_adid, ''), NULLIF(idfa, '')) as device_id,
    CASE
        WHEN NULLIF(gps_adid, '') is not NULL THEN 'gps_adid'
        ELSE 'idfa'
        END
        as device_type
FROM {{ adjust.device_schema }}.s2s_total_acq_last_{{ adjust.device_target_period }}_days
WHERE dwh_company_id = {{ dwh_company_id }}
    AND (order_type = 'real_acquisition' OR order_type = 'reorder')
    AND device_id IS NOT NULL
    AND customer_id IS NOT NULL
;


DROP TABLE IF EXISTS temp_customer_most_recent_device;
CREATE TEMP TABLE temp_customer_most_recent_device AS
SELECT
    rfo.source_id,
    dc.dwh_source_code as source_code,
    rfo.customer_id,
    rfo.order_id,
    rfo.order_date,
    ae.device_id,
    ae.device_type
FROM (
    SELECT
        source_id,
        customer_id,
        order_id,

        -- FIXME: This is the reverse of mapping of
        -- https://github.com/deliveryhero/mkt-data/blob/master/adjust/events_computation/event_computation.sql#L214-L230
        -- Ideally we export all needed information in the event_computation, so we can skip joining to
        -- ranked_fct_order here.
        CASE
            WHEN dwh_company_id = 25 THEN order_number
            WHEN dwh_company_id = 45 THEN country_iso || '_' || order_id
            ELSE order_id
        END AS transaction_id,

        order_date,
        -- Window function to get the last result by order_date.
        ROW_NUMBER() OVER (PARTITION BY source_id, customer_id
            ORDER BY order_date DESC) rank

    FROM {{ il_schema }}.ranked_fct_order
    NATURAL LEFT JOIN dwh_il.dim_countries
    WHERE source_id IN {{ source_id | source_id_filter }}
    AND is_sent
    ) rfo
-- join with dim_countries to exclude deactivated countries
JOIN dwh_il.dim_countries dc
    ON rfo.source_id = dc.source_id
    AND dc.is_active is True
JOIN adjust_events ae
    ON ae.dwh_company_id = dc.dwh_company_id
    -- Assume order id is the unique key
    AND rfo.transaction_id = ae.transaction_id
WHERE rank = 1;


--
-- [MKT-2296](https://jira.deliveryhero.com/browse/MKT-2296)
-- Temporary fix to exclude devices linked to multiple accounts from Audience.
-- We exclude the devices by going back to Adjust raw table `adjust_raw_data_opt`
-- for checking. In the future this shall be integrated into event computation,
-- see [MKT-2297](https://jira.deliveryhero.com/browse/MKT-2297).
--
-- For performance reason we use
--  - Two seperate queries for idfa and gps_adid respectively. This is to prevent the resulting
--    joins to produce nested loops.
--  - Correlated subqueries
--  - Intermediate TEMP tables over CTE to circumvent unsupported correclated subquery error
DROP TABLE IF EXISTS temp_customer_device_mapping_gps_adid;
CREATE TEMP TABLE temp_customer_device_mapping_gps_adid AS
SELECT
    *,
    (
        SELECT COUNT (DISTINCT NULLIF(par_user_id, ''))
        FROM mkt_adjust.adjust_raw_data_opt AS ad
        WHERE dm.device_id = ad.gps_adid
    ) AS accounts_per_device
FROM temp_customer_most_recent_device AS dm
WHERE dm.device_type = 'gps_adid'
;


DROP TABLE IF EXISTS temp_customer_device_mapping_idfa;
CREATE TEMP TABLE temp_customer_device_mapping_idfa AS
SELECT
    *,
    (
        SELECT COUNT (DISTINCT NULLIF(par_user_id, ''))
        FROM mkt_adjust.adjust_raw_data_opt AS ad
        WHERE dm.device_id = ad.idfa
    ) AS accounts_per_device
FROM temp_customer_most_recent_device AS dm
WHERE dm.device_type = 'idfa'
;


-- insert only the new one mapping rows
TRUNCATE TABLE {{ audience_schema }}.{{ brand_code }}_customer_device_mapping;
INSERT INTO {{ audience_schema }}.{{ brand_code }}_customer_device_mapping (
    source_id,
    source_code,
    customer_id,
    order_id,
    order_date,
    device_id,
    device_type,
    accounts_per_device
)
SELECT
    source_id,
    source_code,
    customer_id,
    order_id,
    order_date,
    device_id,
    device_type,
    accounts_per_device
FROM (
    SELECT * FROM temp_customer_device_mapping_gps_adid
    UNION ALL
    SELECT * FROM temp_customer_device_mapping_idfa
)
;


SET analyze_threshold_percent TO 0;
ANALYZE {{ audience_schema }}.{{ brand_code }}_customer_device_mapping
PREDICATE COLUMNS;
