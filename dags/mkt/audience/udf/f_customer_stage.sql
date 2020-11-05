-- f_customer_stage(day_of_computation, total_orders, last_order_ts) -> customer_stage_name
CREATE OR REPLACE FUNCTION {{ audience_schema }}.f_customer_stage (DATE, BIGINT, DATE)
    RETURNS VARCHAR(256)
STABLE
AS $$
/*
    Returns the defined name of the customer stage a given customer is in

    $1 - day of computation - will normally be GETSYSDATE(), but you can set to different date to simulate status
                              from a different date
    $2 - total_orders
    $3 - last_order_ts
 */
SELECT
    CASE
        WHEN $2 = 0 THEN 'prospect'
        WHEN $2 = 1 AND DATEDIFF('days', $3, $1) <= 30 THEN 'welcome'
        WHEN $2 > 1 AND DATEDIFF('days', $3, $1) <= 30 THEN 'active'
        WHEN $2 > 0 AND DATEDIFF('days', $3, $1) BETWEEN 31 AND 90 THEN 'churn-30-90'
        WHEN $2 > 0 AND DATEDIFF('days', $3, $1) BETWEEN 91 AND 180 THEN 'churn-90-180'
        ELSE 'inactive'
    END
$$ LANGUAGE SQL;
