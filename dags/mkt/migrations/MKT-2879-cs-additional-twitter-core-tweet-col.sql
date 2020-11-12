SELECT account_object_id
    , campaign_object_id
    , card_id
    , card_name
    , item_object_id
    , tweet_id
    , tweet_object_id
    , CAST(NULL AS STRING) AS vertical_ad_level
    , created_at
    , created_date
    , _ingested_at
    , merge_layer_run_from
FROM `fulfillment-dwh-production.raw_mkt.twitter_core_tweet`
