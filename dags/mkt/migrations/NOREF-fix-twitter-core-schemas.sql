SELECT account_object_id
    , campaign_object_id
    , card_id
    , card_name
    , item_object_id
    , CAST(tweet_id AS STRING) AS tweet_id
    , tweet_object_id
    , created_at
    , created_date
    , _ingested_at
    , merge_layer_run_from
FROM `fulfillment-dwh-production.raw_mkt.twitter_core_tweet`

SELECT account_object_id
    , ad_group
    , CAST(ad_group_id AS STRING) AS ad_group_id
    , campaign_object_id
    , item_object_id
    , placements
    , created_at
    , created_date
    , _ingested_at
    , merge_layer_run_from
FROM `fulfillment-dwh-production.raw_mkt.twitter_core_item`
