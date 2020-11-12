TRUNCATE {{ audience_schema }}.{{ brand_code }}_audience_definition;
INSERT INTO {{ audience_schema }}.{{ brand_code }}_audience_definition (
    source_id,
    source_code,
    stage,
    variant,
    channel,
    campaign,
    days_in_stage_enter,
    days_in_stage_exit,
    valid_from,
    valid_until
)
VALUES
    -- stage = welcome
    (110, 'FP_TH', 'welcome', 'A', 'crm', 'C1', 1, 30, '2020-07-15', '2020-10-18'),
    (110, 'FP_TH', 'welcome', 'A', 'pm', 'C1', 1, 30, '2020-07-15', '2020-10-18'),
    (110, 'FP_TH', 'welcome', 'B', 'crm', 'C1', 1, 30, '2020-07-15', '2020-10-18'),
    (110, 'FP_TH', 'welcome', 'B', 'pm', 'None', 1, 30, '2020-07-15', '2020-10-18'),

    -- stage = active-15-30
    (104, 'FP_PH', 'active', 'A', 'crm', 'C1', 15, 30, '2020-08-01', '2020-10-18'),
    (104, 'FP_PH', 'active', 'A', 'pm', 'C1', 15, 30, '2020-08-01', '2020-10-18'),
    (104, 'FP_PH', 'active', 'B', 'crm', 'C1', 15, 30, '2020-08-01', '2020-10-18'),
    (104, 'FP_PH', 'active', 'B', 'pm', 'None', 15, 30, '2020-08-01', '2020-10-18')
;

INSERT INTO {{ audience_schema }}.{{ brand_code }}_audience_definition (
    source_id,
    source_code,
    stage,
    variant,
    days_in_stage_enter,
    days_in_stage_exit,
    loyalty_status_all_verts,
    total_non_restaurant_orders,
    channel,
    campaign,
    valid_from,
    valid_until
)
VALUES
    -- FP_SG, stage = active, loyals with zero non-restaurant orders
    (109, 'FP_SG', 'active', 'A', 1, 30, 'Loyal', 0, 'crm', 'loyal_restaurant-only', '2020-10-18', NULL),
    (109, 'FP_SG', 'active', 'A', 1, 30, 'Loyal', 0, 'pm', 'loyal_restaurant-only', '2020-10-18', NULL),
    (109, 'FP_SG', 'active', 'B', 1, 30, 'Loyal', 0, 'crm', 'loyal_restaurant-only', '2020-10-18', NULL),
    (109, 'FP_SG', 'active', 'B', 1, 30, 'Loyal', 0, 'pm', 'None', '2020-10-18', NULL)
;
