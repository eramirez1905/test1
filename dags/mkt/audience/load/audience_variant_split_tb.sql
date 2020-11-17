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
    (42, 'TB_AE', 'welcome', 'A', 0.0, 0.7, '2020-06-01', NULL),
    (42, 'TB_AE', 'welcome', 'B', 0.7, 1.0, '2020-06-01', NULL),

    (44, 'TB_KW', 'welcome', 'A', 0.0, 0.6, '2020-06-01', NULL),
    (44, 'TB_KW', 'welcome', 'B', 0.6, 1.0, '2020-06-01', NULL)
;
