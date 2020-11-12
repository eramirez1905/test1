CREATE TEMPORARY FUNCTION calculate_nps_flag(import_id STRING, question_id STRING)
RETURNS INT64 AS(
  CASE
    WHEN import_id = 'QID38' THEN 1
    WHEN (import_id IN ('QID39', 'QID109') AND question_id = 'Q1.2')
      THEN 2
    ELSE NULL
  END
);

CREATE TEMPORARY FUNCTION calculate_customer_type(nps_score INT64)
RETURNS STRING AS(
  CASE
    WHEN nps_score > 8 THEN 'Promoter'
    WHEN nps_score > 6 AND nps_score < 9 THEN 'Passive'
    WHEN nps_score >= 0 AND nps_score < 7 THEN 'Detractor'
    ELSE NULL
  END
);

CREATE TEMPORARY FUNCTION calculate_nps_reason(nps_reason STRING)
RETURNS STRING AS(
  CASE
    WHEN nps_reason = 'Delivery experience' THEN 'Delivery Experience'
    WHEN nps_reason LIKE 'Waiting and Delivery of meal%' THEN 'Delivery Experience'
    WHEN nps_reason LIKE 'Receiving and Consuming your Meal%' THEN 'Receiving and Consuming your Meal'
    WHEN nps_reason LIKE 'Ordering process%' THEN 'Ordering Process'
    WHEN nps_reason LIKE 'Checkout process%' THEN 'Checkout Process'
    WHEN nps_reason LIKE 'foodpanda%' THEN 'Foodpanda Brand'
    WHEN nps_reason LIKE 'Contact Center%' THEN 'Contact Center'
    WHEN nps_reason LIKE 'Failed Order%' THEN 'Failed Order'
    WHEN nps_reason IS NULL THEN 'No Reason Selected'
    ELSE nps_reason
  END
);

CREATE TEMPORARY FUNCTION translate_answer_import_id(import_id STRING)
RETURNS STRING AS(
  CASE
    WHEN import_id = 'QID102-1' THEN 'Delivery Experience Duration'
    WHEN import_id = 'QID102-7' THEN 'Order Status Communication'
    WHEN import_id = 'QID102-10' THEN 'Tracking Functionality'
    WHEN import_id = 'QID102-11' THEN 'Delivery Courier'
    WHEN import_id = 'QID102-12' THEN 'Food Packaging'
    WHEN import_id = 'QID102-13' THEN 'Order Status Page'
    WHEN import_id = 'QID102-14' THEN 'Actual vs Expected Time'
    WHEN import_id = 'QID102-15' THEN 'Rider Communication'
    WHEN import_id = 'QID102-16' THEN 'Rider Presentation'
    ELSE import_id
  END
);

CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._nps_survey_response`
PARTITION BY created_date AS
WITH nps_data AS(
  SELECT DISTINCT content.survey_id AS survey_id
    , content.global_entity_id AS nps_entity_id
    , content.timestamp AS survey_create_datetime
    , content.after_order_response.order_id AS nps_order_id
    , DATE(content.after_order_response.start_date) AS survey_start_date
    , content.after_order_response.start_date AS survey_start_datetime
    , content.after_order_response.finished AS is_finished
    , content.after_order_response.response.response_id AS response_id
    , a.question_id AS question_id
    , a.import_id AS import_id
    , a.option_id AS option_id
    , a.answer AS answer
    , calculate_nps_flag(import_id, question_id) AS nps_flag
  FROM `{{ params.project_id }}.dl.data_fridge_survey_response_stream` nps
  LEFT JOIN UNNEST(content.after_order_response.response.answers) a
  WHERE created_date >= '2020-02-08'
    --  Logistics considers only AFTER_ORDER responses.
    AND content.survey_response_type = 'AFTER_ORDER'
    --  QID83-% user agent information and hence not needed
    AND a.import_id NOT LIKE 'QID83-%'
    --  Remove test data
    AND global_entity_id NOT IN ('TEST_INSTRUMENTATION_VA', 'TEST_VA')
), nps_score_data AS(
  SELECT survey_id
    , nps_entity_id
    , survey_create_datetime
    , nps_order_id
    , survey_start_date
    , survey_start_datetime
    , is_finished
    , response_id
    , question_id
    , import_id
    , SAFE_CAST(option_id AS INT64) as nps_score
    , nps_flag
  FROM nps_data
  -- NPS score corresponds to QID38
  WHERE import_id = 'QID38'
), nps_reason_data AS(
  SELECT survey_id
    , nps_entity_id
    , survey_create_datetime
    , nps_order_id
    , survey_start_date
    , survey_start_datetime
    , is_finished
    , response_id
    , question_id
    , import_id
    , option_id as nps_reason
  FROM nps_data
  --  NPS Reason corresponds to QID39/QID109 based on entities.
  WHERE import_id IN ('QID39', 'QID109')
    AND question_id = 'Q1.2'
), nps_answers_data AS (
  SELECT survey_id
    , nps_entity_id AS entity_id
    , survey_create_datetime
    , nps_order_id
    , survey_start_date
    , survey_start_datetime
    , is_finished
    , response_id
    , question_id
    , import_id
    , option_id
    , answer
  FROM nps_data
  WHERE nps_flag IS NULL
), order_data AS(
  SELECT created_date
    , c.region AS region
    , o.country_code AS country_code
    , c.country_name AS country_name
    , o.city_id AS city_id
    , ci.name AS city_name
    , entity.id AS orders_entity_id
    , entity.display_name AS entity_display_name
    , vendor.id AS vendor_id
    , vendor.name AS vendor_name
    , o.platform_order_code AS platform_order_code
    , o.timezone
    , d.delivery_status AS delivery_status
    , d.stacked_deliveries AS stacked_deliveries
    , ROUND(IF(o.is_preorder IS FALSE, d.timings.assumed_actual_preparation_time / 60, 0),2)
        AS assumed_actual_preparation_time_mins
    , ROUND(IF(o.is_preorder IS FALSE, o.timings.actual_delivery_time / 60, 0), 2) AS actual_delivery_time_mins
    , ROUND(IF(o.is_preorder IS FALSE, o.timings.promised_delivery_time / 60, 0), 2) AS promised_delivery_time_mins
    , ROUND((o.timings.order_delay / 60), 2) AS order_delay_mins
    , ROUND((d.dropoff_distance_manhattan / 1000), 2) AS dropoff_distance_manhattan_kms
  FROM `{{ params.project_id }}.cl.orders` o
  LEFT JOIN UNNEST(deliveries) d ON d.is_primary IS TRUE
  LEFT JOIN `{{ params.project_id }}.cl.countries` c ON o.country_code = c.country_code
  LEFT JOIN UNNEST(c.cities) ci ON o.city_id = ci.id
  --  Data is correct in the data fridge post '2020-02-07'
  WHERE created_date >= '2020-02-08'
), agg_data AS(
  SELECT survey_id
    , region
    , country_code
    , country_name
    , city_id
    , city_name
    , nps_entity_id AS entity_id
    , entity_display_name
    , s.survey_create_datetime
    , created_date
    , nps_order_id
    , platform_order_code
    , s.survey_start_date
    , s.survey_start_datetime
    , timezone
    , s.is_finished
    , response_id
    , vendor_id
    , vendor_name
    , IF(platform_order_code IS NULL, 'Vendor Delivery', 'Own Delivery') AS delivery_type
    , s.import_id AS score_import_id
    , r.import_id AS reason_import_id
    , nps_score
    --    NPS Reason for delivery experience varies by countries
    , IF((nps_reason IN ('Delivery Experience', 'Delivery experience') OR
        nps_reason LIKE 'Waiting and Delivery of meal%') , TRUE, FALSE) AS is_delivery_experience
    , delivery_status
    , stacked_deliveries
    , assumed_actual_preparation_time_mins
    , actual_delivery_time_mins
    , promised_delivery_time_mins
    , order_delay_mins
    , dropoff_distance_manhattan_kms
    , nps_flag
    , calculate_customer_type(nps_score) AS customer_type
    , calculate_nps_reason(nps_reason) AS nps_reason
  FROM nps_score_data s
  LEFT JOIN nps_reason_data r USING (survey_id, nps_entity_id, nps_order_id, response_id)
  LEFT JOIN order_data ON nps_entity_id = orders_entity_id
    AND nps_order_id = platform_order_code
), agg_answers_data AS (
  SELECT entity_id
    , response_id
    , survey_start_date
    , ARRAY_AGG(
        STRUCT(
          import_id
          , translate_answer_import_id(import_id) AS detailed_reason
          , IF(option_id IS NULL, answer, option_id) AS option_id
          , answer
        )
      ) AS details
  FROM nps_answers_data
  GROUP BY 1, 2, 3
)
SELECT survey_id
  , region
  , country_code
  , country_name
  , city_id
  , city_name
  , entity_id
  , entity_display_name
  , survey_create_datetime
  , survey_start_date
  , survey_start_datetime
  , timezone
  --  created date is NULL for vendor delivery
  , IF(created_date IS NULL, survey_start_date, created_date) AS created_date
  , platform_order_code
  , nps_order_id
  , response_id
  , vendor_id
  , vendor_name
  , score_import_id
  , reason_import_id
  , nps_score
  , nps_reason
  , customer_type
  , is_finished
  , is_delivery_experience
  , delivery_type
  , delivery_status
  , stacked_deliveries
  , assumed_actual_preparation_time_mins
  , actual_delivery_time_mins
  , promised_delivery_time_mins
  , order_delay_mins
  , dropoff_distance_manhattan_kms
  , details
FROM agg_data a
LEFT JOIN agg_answers_data aa USING (entity_id, response_id, survey_start_date)
