TRUNCATE {{ audience_schema }}.{{ brand_code }}_audience_variant_split;
INSERT INTO {{ audience_schema }}.{{ brand_code }}_audience_variant_split (
    source_id,
    source_code,
    stage,
    variant,
    split_from, -- lower boundary, inclusive
    split_to, -- upper boundary, exclusive
    valid_from,
    valid_until
)
VALUES
    -- stage = welcome
    (110, 'FP_TH', 'welcome', 'A', 0.0, 0.7, '2020-07-15', '2020-10-18'),
    (110, 'FP_TH', 'welcome', 'B', 0.7, 1.0, '2020-07-15', '2020-10-18'),

    -- stage = active-15-30
    (104, 'FP_PH', 'active', 'A', 0.0, 0.7, '2020-08-01', '2020-10-18'),
    (104, 'FP_PH', 'active', 'B', 0.7, 1.0, '2020-08-01', '2020-10-18'),

    -- stage = active-15-30
    (109, 'FP_SG', 'active', 'A', 0.0, 0.5, '2020-10-18', NULL),
    (109, 'FP_SG', 'active', 'B', 0.5, 1.0, '2020-10-18', NULL)
;
