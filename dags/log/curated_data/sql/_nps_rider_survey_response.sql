CREATE TEMPORARY FUNCTION calculate_nps_flag(import_id STRING, question_id STRING)
RETURNS INT64 AS(
  CASE
    WHEN import_id = 'QID156' THEN 1
    WHEN (import_id IN ('QID407') AND question_id = 'Q1.2')
      THEN 2
    ELSE NULL
  END
);

CREATE TEMPORARY FUNCTION calculate_rider_type(nps_score INT64)
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
    WHEN nps_reason LIKE 'Managing shifts%' THEN 'Managing Shifts'
    WHEN nps_reason LIKE 'Making deliveries%' THEN 'Making Deliveries'
    WHEN nps_reason LIKE '%Support%' THEN 'Support'
    WHEN nps_reason LIKE 'Payments%' OR nps_reason LIKE 'Service fee%' THEN 'Payment'
    WHEN nps_reason LIKE 'Additional Benefits%' THEN 'Additional Benefits'
    WHEN nps_reason LIKE 'Placeholder%' OR nps_reason LIKE 'Other%' THEN 'Other'
    WHEN nps_reason LIKE 'Application process%' THEN 'Application Process'
    WHEN nps_reason LIKE 'Onboarding process%' THEN 'Onboarding Process'
    WHEN nps_reason LIKE 'Expectations vs Reality%' OR nps_reason LIKE 'Expectations about job%' THEN 'Expectations vs Reality'
    WHEN nps_reason IS NULL THEN 'No Reason Selected'
    ELSE nps_reason
  END
);

CREATE TEMPORARY FUNCTION translate_answer_import_id(import_id STRING)
RETURNS STRING AS(
  CASE
    WHEN import_id = 'QID390-2' THEN 'Activities - Shift Selection App'
    WHEN import_id = 'QID390-4' THEN 'Activities - Shift selection process'
    WHEN import_id = 'QID390-17' THEN 'Activities - Availability of shifts'
    WHEN import_id = 'QID390-18' THEN 'Activities - Visibility of shifts for next week'
    WHEN import_id = 'QID390-19' THEN 'Activities - Shift cancel'
    WHEN import_id = 'QID351-7' THEN 'Deliveries - Interactions with customers'
    WHEN import_id = 'QID351-8' THEN 'Deliveries - Interactions with restaurants'
    WHEN import_id = 'QID351-9' THEN 'Deliveries - Pick-up/ drop-off distance'
    WHEN import_id = 'QID351-14' THEN 'Deliveries - RoadRunner App'
    WHEN import_id = 'QID351-15' THEN 'Deliveries - Equipment'
    WHEN import_id = 'QID415-TEXT' THEN 'Activities - Comments'
    WHEN import_id = 'QID416-TEXT' THEN 'Comments'
    WHEN import_id = 'QID417-TEXT' THEN 'Comments'
    WHEN import_id = 'QID327-1' THEN 'Support - Dispatcher'
    WHEN import_id = 'QID327-15' THEN 'Support - Delivery Partner Portal'
    WHEN import_id = 'QID327-16' THEN 'Support - Newsletter'
    WHEN import_id = 'QID327-17' THEN 'Support - Agent availability'
    WHEN import_id = 'QID327-18' THEN 'Support - Speed response'
    WHEN import_id = 'QID327-19' THEN 'Support - Rider portal'
    WHEN import_id = 'QID327-20' THEN 'Support - Fresh Chat'
    WHEN import_id = 'QID327-21' THEN 'Support - Delivery Partner'
    WHEN import_id = 'QID327-22' THEN 'Support - Rider Captain'
    WHEN import_id = 'QID395-1' THEN 'Support - Access to relevant information about your day-to-day activities'
    WHEN import_id = 'QID395-3' THEN 'Support - Timely communicated news/ changes'
    WHEN import_id = 'QID395-4' THEN 'Support - Rider Community'
    WHEN import_id = 'QID395-15' THEN 'Support - Agent kindness'
    WHEN import_id = 'QID395-16' THEN 'Support - Problem Resolution'
    WHEN import_id = 'QID396-TEXT' THEN 'Support - Comments'
    WHEN import_id = 'QID374-TEXT' THEN 'Support - Comments'
    WHEN import_id = 'QID335-21' THEN 'Payment - Service Fee'
    WHEN import_id = 'QID335-22' THEN 'Payment - Cash Collection'
    WHEN import_id = 'QID335-23' THEN 'Payment - Cash Deposit'
    WHEN import_id = 'QID335-24' THEN 'Payment - Received correct service fee amount'
    WHEN import_id = 'QID335-25' THEN 'Payment - Ability to have a stable monthly income'
    WHEN import_id = 'QID335-26' THEN 'Payment - Process to get tips'
    WHEN import_id = 'QID410-TEXT' THEN 'Payment - Comments'
    WHEN import_id = 'QID398-TEXT' THEN 'Comments'
    WHEN import_id = 'QID161-5' THEN 'Application - Clarity of the job requirements'
    WHEN import_id = 'QID161-8' THEN 'Application - Upload required documents'
    WHEN import_id = 'QID161-9' THEN 'Application - How much the job matched my initial expectations'
    WHEN import_id = 'QID161-11' THEN 'Application - Information about the job before applying'
    WHEN import_id = 'QID161-16' THEN 'The process was easy'
    WHEN import_id = 'QID370-TEXT' THEN 'Application - Comments'
    WHEN import_id = 'QID326-2' THEN 'Onboarding - The length of the onboarding process'
    WHEN import_id = 'QID326-5' THEN 'Onboarding - Amount of information available in training manuals'
    WHEN import_id = 'QID326-16' THEN 'Onboarding - How quickly I received my equipment'
    WHEN import_id = 'QID326-17' THEN 'Onboarding - How quickly I was able to take the first shift after applying'
    WHEN import_id = 'QID371-TEXT' THEN 'Onboarding - Comments'
    WHEN import_id = 'QID410-1' THEN 'Applying - To have a job with career development opportunities'
    WHEN import_id = 'QID410-2' THEN 'Applying - To have a secured job'
    WHEN import_id = 'QID410-3' THEN 'Applying - To earn money quickly'
    WHEN import_id = 'QID410-4' THEN 'Applying - To start working fast / quickly'
    WHEN import_id = 'QID410-5' THEN 'Applying - To have flexibility'
    WHEN import_id = 'QID410-6' THEN 'Applying - To be able to plan my shifts in advance'
    WHEN import_id = 'QID410-7' THEN 'Applying - To be able to exercise and do something I like (be outdoors/riding a bike/driving) while earning money'
    WHEN import_id = 'QID410-8' THEN 'Applying - To join a community with similar interests'
    WHEN import_id = 'QID410-9' THEN 'Applying - To have a job that could help me support other career goals'
    WHEN import_id = 'QID410-10' THEN 'Applying - To a have a job with good working conditions'
    WHEN import_id = 'QID410-11' THEN 'Applying - To have a job that doesn`t require to speak the local language'
    WHEN import_id = 'QID410-12' THEN 'Applying - To have additional benefits (tips, bonus)'
    WHEN import_id = 'QID410-10' THEN 'Applying - To a have a job with good working conditions'
    WHEN import_id = 'QID398-TEXT' THEN 'Applying - Comments'
    WHEN import_id = 'QID411-TEXT' THEN 'Comments'
    ELSE import_id
  END
);

CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._nps_rider_survey_response`
PARTITION BY created_date AS
WITH nps_data AS(
  -- SELECT DISTINCT to avoid duplicates due to a metadata.tracking.id
  SELECT DISTINCT content.survey_id AS survey_id
    , SPLIT(content.survey_response.rider_id, '_')[OFFSET(1)] AS rider_id
    , LOWER(content.survey_response.country_code) AS country_code
    , content.timestamp AS survey_create_datetime
    , content.survey_response.start_date AS survey_start_datetime
    , content.survey_response.end_date AS survey_end_datetime
    , created_date
    , content.survey_response.finished AS is_finished
    , 'rider_survey' AS source_type
    , content.survey_response.response.response_id AS response_id
    , a.question_id AS question_id
    , a.import_id AS import_id
    , a.option_id AS option_id
    , a.answer AS answer
    , calculate_nps_flag(import_id, question_id) AS nps_flag
  FROM `{{ params.project_id }}.dl.data_fridge_rider_survey_stream` nps
  LEFT JOIN UNNEST(content.survey_response.response.answers) a

  UNION ALL
  -- SELECT DISTINCT to avoid duplicates due to a metadata.tracking.id
  SELECT DISTINCT content.survey_id AS survey_id
    , SPLIT(content.survey_response.rider_id, '_')[SAFE_OFFSET(1)] AS rider_id
    , LOWER(content.survey_response.country_code) AS country_code
    , content.timestamp AS survey_create_datetime
    , content.survey_response.start_date AS survey_start_datetime
    , content.survey_response.end_date AS survey_end_datetime
    , created_date
    , content.survey_response.finished AS is_finished
    , 'new_rider_survey' AS source_type
    , content.survey_response.response.response_id AS response_id
    , a.question_id AS question_id
    , a.import_id AS import_id
    , a.option_id AS option_id
    , a.answer AS answer
    , calculate_nps_flag(import_id, question_id) AS nps_flag
  FROM `{{ params.project_id }}.dl.data_fridge_new_rider_survey_stream` nps
  LEFT JOIN UNNEST(content.survey_response.response.answers) a

), nps_score_data AS (
  SELECT survey_id
    , rider_id
    , country_code
    , survey_create_datetime
    , survey_start_datetime
    , survey_end_datetime
    , created_date
    , is_finished
    , source_type
    , response_id
    , question_id
    , import_id
    , SAFE_CAST(option_id AS INT64) as nps_score
    , nps_flag
  FROM nps_data
  -- NPS score corresponds to QID156
  WHERE import_id = 'QID156'
), nps_reason_data AS(
  SELECT survey_id
    , rider_id
    , country_code
    , survey_create_datetime
    , survey_start_datetime
    , survey_end_datetime
    , created_date
    , is_finished
    , source_type
    , response_id
    , question_id
    , import_id
    , option_id AS nps_reason
  FROM nps_data
  -- NPS Reason corresponds to QID407.
  WHERE import_id = 'QID407'
    AND question_id = 'Q1.2'
), nps_answers_data AS (
  SELECT survey_id
    , rider_id
    , country_code
    , survey_create_datetime
    , survey_start_datetime
    , survey_end_datetime
    , created_date
    , is_finished
    , source_type
    , response_id
    , question_id
    , import_id
    , option_id
    , answer
  FROM nps_data
  WHERE nps_flag IS NULL
), agg_data AS(
  SELECT survey_id
    , rider_id
    , country_code
    , s.survey_create_datetime
    , s.survey_start_datetime
    , s.survey_end_datetime
    , s.created_date
    , s.is_finished
    , s.source_type
    , response_id
    , s.question_id
    , s.import_id AS score_import_id
    , r.import_id AS reason_import_id
    , nps_score
    , nps_flag
    , calculate_rider_type(nps_score) AS rider_type
    , calculate_nps_reason(nps_reason) AS nps_reason
  FROM nps_score_data s
  LEFT JOIN nps_reason_data r USING (survey_id, rider_id, response_id, country_code)
), agg_answers_data AS (
  SELECT rider_id
    , country_code
    , response_id
    , survey_start_datetime
    , source_type
    , ARRAY_AGG(
      STRUCT (translate_answer_import_id(import_id) AS detailed_reason
        , COALESCE(option_id, answer) AS option_id
        , answer
      )
    ) AS details
  FROM nps_answers_data
  GROUP BY 1, 2, 3, 4, 5
)
SELECT survey_id
  , SAFE_CAST(rider_id AS INT64) AS rider_id
  , country_code
  , survey_create_datetime
  , survey_start_datetime
  , survey_end_datetime
  , created_date
  , source_type
  , response_id
  , nps_score
  , nps_reason
  , rider_type
  , is_finished
  , details
FROM agg_data a
LEFT JOIN agg_answers_data aa USING (response_id, survey_start_datetime, rider_id, country_code, source_type)
