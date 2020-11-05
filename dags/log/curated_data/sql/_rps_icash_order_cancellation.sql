CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._rps_icash_order_cancellation`
PARTITION BY created_date AS
WITH icash_delivery_cancellation_dataset AS (
  -- temporary solution due to duplicated data coming from product
  -- issue: orders with more than one cancellation record
  SELECT *
  FROM (
    SELECT region
      , created_date
      , belongs_to
      , delivery_id
      , source
      , owner
      , reason
      , stage
      , ROW_NUMBER() OVER (PARTITION BY region, delivery_id ORDER BY timestamp DESC) AS _row_number
    FROM `{{ params.project_id }}.dl.icash_delivery_cancellation`
  )
  WHERE _row_number = 1
), icash_order_cancellation AS (
  SELECT region
    , created_date
    , belongs_to AS vendor_id
    , delivery_id AS order_id
    , CASE
        WHEN source = 0
          THEN 'CONTACT_CENTER'
        WHEN source = 1
          THEN 'CONTACT_CENTER_VENDOR'
        WHEN source = 2
          THEN 'CONTACT_CENTER_CUSTOMER'
        WHEN source = 3
          THEN 'ORDER_TRACKING_WEBSITE'
        WHEN source = 4
          THEN 'CUSTOMER_NOTIFICATION'
        WHEN source = 5
          THEN 'PLATFORM'
        WHEN source = 6
          THEN 'VENDOR_DEVICE'
        WHEN source = 7
          THEN 'LOGISTICS'
        WHEN source = 8
          THEN 'RPS'
      END AS source
    , CASE
        WHEN owner = 0
          THEN 'CUSTOMER'
        WHEN owner = 1
          THEN 'TRANSPORT'
        WHEN owner = 2
          THEN 'VENDOR'
        WHEN owner = 3
          THEN 'PLATFORM'
      END AS owner
    , (owner = 2) AS is_cancelled_by_vendor
    , CASE
        WHEN reason = 0
          THEN 'ACKNOWLEDGEMENT_TIMEOUT'
        WHEN reason = 1
          THEN 'ADDRESS_INCOMPLETE_MISSTATED'
        WHEN reason = 2
          THEN 'BAD_LOCATION'
        WHEN reason = 3
          THEN 'BAD_WEATHER'
        WHEN reason = 4
          THEN 'BILLING_PROBLEM'
        WHEN reason = 5
          THEN 'BLACKLISTED'
        WHEN reason = 6
          THEN 'CARD_READER_NOT_AVAILABLE'
        WHEN reason = 7
          THEN 'CLOSED'
        WHEN reason = 8
          THEN 'CONTENT_WRONG_MISLEADING'
        WHEN reason = 9
          THEN 'COURIER_ACCIDENT'
        WHEN reason = 10
          THEN 'COURIER_UNREACHABLE'
        WHEN reason = 11
          THEN 'DELIVERY_ETA_TOO_LONG'
        WHEN reason = 12
          THEN 'DUPLICATE_ORDER'
        WHEN reason = 13
          THEN 'EXTRA_CHARGE_NEEDED'
        WHEN reason = 14
          THEN 'FOOD_QUALITY_SPILLAGE'
        WHEN reason = 15
          THEN 'FRAUD_PRANK'
        WHEN reason = 16
          THEN 'ITEM_UNAVAILABLE'
        WHEN reason = 17
          THEN 'LATE_DELIVERY'
        WHEN reason = 18
          THEN 'MENU_ACCOUNT_SETTINGS'
        WHEN reason = 19
          THEN 'MISTAKE_ERROR'
        WHEN reason = 20
          THEN 'MOV_NOT_REACHED'
        WHEN reason = 21
          THEN 'NEVER_DELIVERED'
        WHEN reason = 22
          THEN 'NO_COURIER'
        WHEN reason = 23
          THEN 'NO_RESPONSE'
        WHEN reason = 24
          THEN 'ORDER_MODIFICATION_NOT_POSSIBLE'
        WHEN reason = 25
          THEN 'OUTSIDE_DELIVERY_AREA'
        WHEN reason = 26
          THEN 'OUTSIDE_SERVICE_HOURS'
        WHEN reason = 27
          THEN 'REASON_UNKNOWN'
        WHEN reason = 28
          THEN 'TECHNICAL_PROBLEM'
        WHEN reason = 29
          THEN 'TEST_ORDER'
        WHEN reason = 30
          THEN 'TOO_BUSY'
        WHEN reason = 31
          THEN 'UNABLE_TO_FIND'
        WHEN reason = 32
          THEN 'UNABLE_TO_PAY'
        WHEN reason = 33
          THEN 'UNPROFESSIONAL_BEHAVIOUR'
        WHEN reason = 34
          THEN 'UNREACHABLE'
        WHEN reason = 35
          THEN 'VOUCHER_NOT_APPLIED'
        WHEN reason = 36
          THEN 'WILL_NOT_WORK_WITH_PLATFORM'
        WHEN reason = 37
          THEN 'WRONG_ORDER_ITEMS_DELIVERED'
      END AS reason
    , CASE
        WHEN stage = 0
          THEN 'CONFIRMATION'
        WHEN stage = 1
          THEN 'PREPARATION'
        WHEN stage = 2
          THEN 'DELIVERY'
        WHEN stage = 3
          THEN 'POST_DELIVERY'
      END AS stage
  FROM icash_delivery_cancellation_dataset
)
SELECT region
  , created_date
  , vendor_id
  , order_id
  , STRUCT(
      reason
      , owner
      , is_cancelled_by_vendor
      , stage
      , source
    ) AS cancellation
FROM icash_order_cancellation
