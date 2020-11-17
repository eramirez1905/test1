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
    (42, 'TB_AE', 'welcome', 'A', 'crm', 'C1', 1, 23, '2020-06-01', NULL),
    (42, 'TB_AE', 'welcome', 'A', 'crm', 'None', 24, 30, '2020-06-01', NULL),
    (42, 'TB_AE', 'welcome', 'A', 'pm', 'C1', 1, 23, '2020-06-01', NULL),
    (42, 'TB_AE', 'welcome', 'A', 'pm', 'None', 24, 30, '2020-06-01', NULL),
    (42, 'TB_AE', 'welcome', 'B', 'crm', 'C1', 1, 23, '2020-06-01', NULL),
    (42, 'TB_AE', 'welcome', 'B', 'crm', 'None', 24, 30, '2020-06-01', NULL),

    (44, 'TB_KW', 'welcome', 'A', 'crm', 'C1', 1, 23, '2020-06-01', NULL),
    (44, 'TB_KW', 'welcome', 'A', 'crm', 'None', 24, 30, '2020-06-01', NULL),
    (44, 'TB_KW', 'welcome', 'A', 'pm', 'C1', 1, 23, '2020-06-01', NULL),
    (44, 'TB_KW', 'welcome', 'A', 'pm', 'None', 24, 30, '2020-06-01', NULL),
    (44, 'TB_KW', 'welcome', 'B', 'crm', 'C1', 1, 23, '2020-06-01', NULL),
    (44, 'TB_KW', 'welcome', 'B', 'crm', 'None', 24, 30, '2020-06-01', NULL)
;
