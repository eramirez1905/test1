
INSERT INTO {{ ncr_schema }}.{{ brand_code }}_braze_history
SELECT
    *
FROM {{ ncr_schema }}.{{ brand_code }}_braze_main
;

ANALYZE {{ ncr_schema }}.{{ brand_code }}_braze_history;
