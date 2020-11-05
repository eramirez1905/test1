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
    -- MKT-2215: To define new logics for Audience is necessary to add the date the new
    -- logic will be working in production for `valid_from` and set `valid_until` field
    -- to NULL. This way we can assume the updated row as an ACTIVE AUDIENCE.
    --
    -- By doing this we are able to keep a history of changes through out time and don't
    -- need to apply any logics or changes to `customer_audience.sql` because it is
    -- already using the only active Audience logic by filtering `valid_until IS NULL`.
    --

    -- stage = welcome
    (9, 'PY_AG', 'welcome', 'A', 'crm', 'C1', 1, 20, '2020-01-01', '2020-03-08'),
    (9, 'PY_AG', 'welcome', 'A', 'crm', 'None', 21, 30, '2020-01-01', '2020-03-16'), -- Mistakenly only disabled on '2020-03-16' while it should have been disabled on '2020-03-08'
    (9, 'PY_AG', 'welcome', 'A', 'crm', 'C1', 1, 30, '2020-03-09', NULL),
    (9, 'PY_AG', 'welcome', 'A', 'pm', 'C2', 8, 30, '2020-01-01', NULL),
    (9, 'PY_AG', 'welcome', 'B', 'crm', 'C1', 1, 30, '2020-01-01', NULL),
    (9, 'PY_AG', 'welcome', 'B', 'pm', 'None', 1, 30, '2020-01-01', NULL),

    -- stage = churn-30-90
    (9, 'PY_AG', 'churn-30-90', 'A', 'crm', 'C1', 1, 60, '2020-03-01', NULL),
    (9, 'PY_AG', 'churn-30-90', 'A', 'pm', 'None', 1, 60, '2020-03-01', NULL)
;
