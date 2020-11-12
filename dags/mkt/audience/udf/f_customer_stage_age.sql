-- f_customer_stage_age(day_of_computation, total_orders, last_order_ts) -> customer_stage_age
CREATE OR REPLACE FUNCTION {{ audience_schema }}.f_customer_stage_age (DATE, BIGINT, DATE)
    RETURNS BIGINT
STABLE
AS $$
/*
    Returns the number of days a customer has been in a stage (as returned by f_customer_stage)

    $1 - day of computation - will normally be GETSYSDATE(), but you can set to different date to simulate status
                              from a different date
    $2 - total_orders
    $3 - last_order_ts
 */
SELECT
    CASE
        WHEN $2 = 0 THEN NULL
        WHEN $2 = 1 AND DATEDIFF('days', $3, $1) <= 30 THEN DATEDIFF('days', $3, $1)
        WHEN $2 > 1 AND DATEDIFF('days', $3, $1) <= 30 THEN DATEDIFF('days', $3, $1)
        WHEN $2 > 0 AND DATEDIFF('days', $3, $1) BETWEEN 31 AND 90 THEN DATEDIFF('days', $3, $1) - 30
        WHEN $2 > 0 AND DATEDIFF('days', $3, $1) BETWEEN 91 AND 180 THEN DATEDIFF('days', $3, $1) - 90
        ELSE DATEDIFF('days', $3, $1) - 180
    END
$$ LANGUAGE SQL;
