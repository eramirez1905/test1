-- Mapping table between analytical_customer_id and braze external_id
-- We're using analytical_customer_id as user identifier in NCR, need
-- to map this to external_id to push specific attributes to braze

-- TODO Get only existing braze profiles (from fetcher?) to be able to join
-- with this table while computing user eligibility, that way we'll be able
-- to assign proper amount of notifications.
TRUNCATE {{ ncr_schema }}.{{ brand_code }}_ncr_braze_customer;

INSERT INTO {{ ncr_schema }}.{{ brand_code }}_ncr_braze_customer
-- This mapping is talabat specific, for other brands we should implement
-- it differently (using unique_customer_ids table with is_most_recent_account?)
-- We don't use unique_customer_ids for talabat as customer_id is order_id so
-- is_most_recent_account flag is not correct.
WITH acid_to_external_id_map AS (
SELECT
    source_id,
    analytical_customer_id,
    COALESCE(
        LAST_VALUE(customer_email) IGNORE NULLS OVER (
            PARTITION BY source_id, analytical_customer_id
            ORDER BY order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ),
        CONCAT(
            LAST_VALUE(mobile_number) IGNORE NULLS OVER (
                PARTITION BY source_id, analytical_customer_id
                ORDER BY order_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ),
            '@talabatguest.com'
        )
    ) AS external_id,
    ROW_NUMBER() OVER (
        PARTITION BY source_id, analytical_customer_id
        ORDER BY order_date DESC
    ) AS analytical_customer_id_rank
FROM {{ il_schema }}.ranked_fct_order rfo
JOIN {{ il_schema }}.dim_customer dc
    USING (customer_id, source_id)
WHERE
    is_sent
    -- MKT-2060: exclude whitelabel for Carriage + Talabat = Carabat
    AND is_whitelabel IS NOT TRUE
    AND rfo.source_id IN {{ source_ids|sql_list_filter }}
    AND rfo.analytical_customer_id IS NOT NULL
)
SELECT
    source_id,
    analytical_customer_id AS customer_id,
    external_id AS braze_external_id
FROM acid_to_external_id_map
WHERE analytical_customer_id_rank = 1
    AND external_id IS NOT NULL
;
