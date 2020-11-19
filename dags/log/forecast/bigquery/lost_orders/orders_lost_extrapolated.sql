CREATE TEMP FUNCTION extrapolate_linear(x ARRAY<INT64>)
RETURNS STRUCT<extrapolation FLOAT64, lost_orders FLOAT64>
LANGUAGE js AS
"""
    var data = x.map(function(e, i) {
      return [i, parseInt(e)];
    });
    const sum = [0, 0, 0, 0, 0];
    let len = 0;
    for (let n = 0; n < data.length; n++) {
      if (data[n][1] !== null) {
        len++;
        sum[0] += data[n][0];
        sum[1] += data[n][1];
        sum[2] += data[n][0] * data[n][0];
        sum[3] += data[n][0] * data[n][1];
        sum[4] += data[n][1] * data[n][1];
      }
    }
    const run = ((len * sum[2]) - (sum[0] * sum[0]));
    const rise = ((len * sum[3]) - (sum[0] * sum[1]));
    const gradient = run === 0 ? 0 : rise / run;
    const intercept = (sum[1] / len) - ((gradient * sum[0]) / len);
    const extrapolation = intercept + (x.length + 1) * gradient;
    this.extrapolation = extrapolation;
    this.lost_orders = (extrapolation > 0) ? extrapolation : x.map(function(e) {return parseInt(e)}).sort(function(a,b){return a-b})[x.length-2];
    return this
""";

CREATE OR REPLACE TABLE `lost_orders.orders_lost_extrapolated` AS
WITH linear_extrapolate_model AS (
  SELECT
    country_code,
    event_type,
    event_id,
    zone_id ,
    datetime_starts_at,
    extrapolate_linear(ARRAY_AGG(num_orders ORDER BY bucket_starts_at)).*
  FROM `lost_orders.events_orders_history_all`
  WHERE
    starts_at != bucket_starts_at
  GROUP BY country_code, event_type, event_id, zone_id, datetime_starts_at
  ORDER BY event_id
)
SELECT
  e.country_code,
  e.event_type,
  e.event_id,
  e.zone_id ,
  e.datetime_starts_at AS datetime,
  e.starts_at,
  e.ends_at,
  e.num_orders AS orders_seen,
  lem.extrapolation AS orders_extrapolation,
  lem.lost_orders AS orders_extrapolated,
  CASE
    WHEN lem.lost_orders - e.num_orders < 0 THEN 0
    ELSE ceil(lem.lost_orders - e.num_orders)
  END AS orders_lost_net
FROM `lost_orders.events_orders_history_all` e
LEFT JOIN linear_extrapolate_model lem 
  ON lem.country_code = e.country_code 
  AND lem.event_type = e.event_type
  AND lem.event_id = e.event_id 
  AND lem.zone_id = e.zone_id
  AND lem.datetime_starts_at = e.datetime_starts_at
WHERE
  e.starts_at = bucket_starts_at
