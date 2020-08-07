DROP TABLE `peya-data-origins-stg.origin_data_refined.user_origin`;

CREATE OR REPLACE TABLE
`peya-data-origins-stg.origin_data_refined.user_origin`
 PARTITION BY RANGE_BUCKET(user_country_id, GENERATE_ARRAY(0, 50, 1))
	CLUSTER BY user_id, user_country_id, user_email
	AS 
	SELECT 
          u.id AS user_id
        , u.country.id AS user_country_id
        , u.email AS user_email
        , u.name AS user_name
        , u.lastName AS user_last_name
        , ut.id AS user_type
        , u.data.gender AS user_gender
        , CASE WHEN u.data.birth IS NULL THEN '1900-01-01 00:00:00 UTC' ELSE CAST(u.data.birth AS TIMESTAMP) END AS user_birth
        , 0 AS user_age
        , p.id AS platform_id 
        , u.data.whiteLabelId	 AS white_label_id
        , NULL AS white_label
        , CAST(replace(cast(cast(firstOrderDate AS DATE) as STRING),'-','')AS INT64) AS user_first_order_date_id
        , firstOrderDate AS user_first_order_date
        , CAST(replace(cast(cast(cast(u.data.lastOrderDate AS timestamp) AS DATE) as STRING),'-','')AS INT64) AS user_last_order_date_id
        , CAST(u.data.lastOrderDate AS timestamp) AS user_last_order_date
        , u.data.orderCount AS user_qty_orders
        , isDeleted AS is_deleted
        , NULL as user_segment
        , NULL AS user_migration_id
        , CAST(replace(cast(cast(registeredDate AS DATE) as STRING),'-','')AS INT64) AS user_registered_date_id
        , registeredDate AS user_registered_date
        , mobile AS user_mobile
		, (select min(id) from unnest(addresses)) as address_id 
        , NULL AS user_street
        , NULL AS user_door_number
        , NULL AS user_phone
        , 0 AS user_area_id -- dependendia de addresses y area
        --, 0 AS user_total_amount -- dependencia de query redshift contra orders
        , lastLogin AS user_last_login
        --, FALSE AS cash_only -- dependencia de query redshift contra orders
        --, FALSE AS op_only -- dependencia de query redshift contra orders
        , receivesNewsletter AS receives_newsletter
        , receivesPushNewsletter AS receives_push_newsletter
        , receivesPushOrderConfirmation AS receives_push_notifications
        , receivesSMS AS receives_sms_notifications
        , receivesWhatsappNotifications AS receives_whatsapp_notifications
        , whatsAppUserId AS whatsapp_user_id
        , CASE WHEN u.state IS NULL THEN 0 ELSE us.id END AS user_state_id
        , lastUpdated AS last_updated
        , facebookAutoShare AS fb_autoshare
		, SPLIT(u.disabledReason, '|')[OFFSET(0)] AS disabled_reason
        , CASE WHEN u.disabledReason NOT LIKE '%|%' THEN NULL ELSE SPLIT(u.disabledReason, '|')[OFFSET(1)] END AS disabled_reason_comment
        , CASE WHEN LOWER(ue.data.account_type) = 'facebook' THEN ue.data.account_id ELSE NULL END  AS  user_facebook_id 
        , CASE WHEN LOWER(ue.data.account_type) = 'google' THEN ue.data.account_id ELSE NULL END AS user_google_id 

    FROM `peya-data-origins-stg.origins_data_stg.sqs_user` AS u
        LEFT JOIN `peya-data-origins-stg.origins_data_raw.enum_user_state` as us ON UPPER(u.state)=UPPER(us.name)
        LEFT JOIN `peya-data-origins-stg.origins_data_raw.enum_user_type` as ut ON UPPER(u.type)=UPPER(ut.name)
        LEFT JOIN  `peya-data-origins-stg.origins_data_raw.enum_platform` as p ON UPPER(p.name) = UPPER(u.platform)
        LEFT JOIN `peya-data-origins-stg.origins_data_stg.sqs_userextlogin` as ue ON ue.data.id = u.id;
  --WHERE u.yyyymmdd >= CAST(date_add(CURRENT_DATE(), INTERVAL -2 DAY) as DATE);
