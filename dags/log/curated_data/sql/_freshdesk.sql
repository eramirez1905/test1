CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._freshdesk`
PARTITION BY created_date AS
WITH cleaned_product AS (
  SELECT ticket_id
    , city_ AS city
    , subject
    , status
    , priority
    , source
    , type
    , agent
    , 'group' AS group_field
    , created_date
    , created_time
    , resolved_time
    , closed_time
    , last_updated_time
    , agent_interactions
    , customer_interactions
    , tags
    , survey_result
    , due_by_time
    , first_response_time__in_hrs_ AS first_response_time_in_hours
    , resolution_status
    , first_response_status
    , association_type
    , CASE
        WHEN product = 'HongKong'
          THEN 'Hong Kong'
        WHEN product = 'NetPincér GO Hungary '
          THEN 'Hungary'
        ELSE product
      END AS product
    , freshchat_conversation_id
    , freshchat_rider_id
    , freshchat_agent
    , freshchat_contact_reason
    , full_name
    , title
    , email
    , twitter
    , facebook_profile_id
    , time_zone AS timezone
    , language
    , unique_external_id
    , company_name
    , instance
FROM `{{ params.project_id }}.dl.freshdesk_reports` f
)
SELECT ticket_id
  -- in the source table Saudi Arabia and Austria are stored under instance column and names are reported as codes
  , CASE
      WHEN product = 'No Product' AND instance = 'ksa'
        THEN 'Saudi Arabia'
      WHEN product = 'No Product' AND instance = 'at'
        THEN 'Austria'
      -- for instance sa we are unable to attribute the country, so it will be South America as region is that one
      -- in the future this case won't happen anymore as correct country will be fixed directly in app
      WHEN product = 'No Product' AND instance = 'sa'
        THEN 'South America'
      ELSE product
    END AS country_name
  , city
  , subject
  , status
  , priority
  , source
  , type
  , agent
  , group_field
  , created_date
  , created_time
  , resolved_time
  , closed_time
  , last_updated_time
  , agent_interactions
  , customer_interactions
  , tags
  , survey_result
  , due_by_time
  , first_response_time_in_hours
  , resolution_status
  , first_response_status
  , association_type
  , freshchat_conversation_id
  , freshchat_rider_id
  , freshchat_agent
  , freshchat_contact_reason
  , full_name
  , title
  , email
  , twitter
  , facebook_profile_id
  , timezone
  , language
  , unique_external_id
  , company_name
FROM cleaned_product cp
