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
    (9, 'PY_AG', 'welcome', 'A', 0.0, 0.8, '2020-01-01', NULL),
    (9, 'PY_AG', 'welcome', 'B', 0.8, 1.0, '2020-01-01', NULL),

    -- stage = churn-30-90
    (9, 'PY_AG', 'churn-30-90', 'A', 0.0, 1.0, '2020-01-01', NULL)
;
