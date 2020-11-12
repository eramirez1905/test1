SELECT account_object_id
  , campaign
  , CAST(campaign_id AS STRING) AS campaign_id
  , campaign_object_id
  , os
  , source_id
  , vertical
  , created_at
  , created_date
  , _ingested_at
  , merge_layer_run_from
FROM `fulfillment-dwh-production.raw_mkt.liftoff_core_campaign`
