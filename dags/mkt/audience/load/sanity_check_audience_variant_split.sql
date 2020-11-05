-- Currently the Audience project only supports one Audience per customer.
-- Here we check that currently there is no more than 1 active campaign for the same Audience.
WITH dual AS (
    SELECT 1 AS x UNION ALL SELECT 0 AS x
),
numbers_table AS (
    SELECT
        x1.x * POWER(2, 0)
        + x2.x * POWER(2, 1)
        + x3.x * POWER(2, 2)
        + x4.x * POWER(2, 3)
        + x5.x * POWER(2, 4)
        + x6.x * POWER(2, 5)
        + x7.x * POWER(2, 6)
        + x8.x * POWER(2, 7)
        + x9.x * POWER(2, 8)
        + x10.x * POWER(2, 9)
        + x11.x * POWER(2, 10)
        + x12.x * POWER(2, 11)
        + x13.x * POWER(2, 12)
        + x14.x * POWER(2, 13) AS number
    FROM
        dual AS x1
        CROSS JOIN dual AS x2
        CROSS JOIN dual AS x3
        CROSS JOIN dual AS x4
        CROSS JOIN dual AS x5
        CROSS JOIN dual AS x6
        CROSS JOIN dual AS x7
        CROSS JOIN dual AS x8
        CROSS JOIN dual AS x9
        CROSS JOIN dual AS x10
        CROSS JOIN dual AS x11
        CROSS JOIN dual AS x12
        CROSS JOIN dual AS x13
        CROSS JOIN dual AS x14
    WHERE
        number BETWEEN 1 and 1000
),
random_num_table AS (
    SELECT
      (number-1) / 1000.0 AS random_num
    FROM numbers_table
)
    SELECT
        random_num,
        source_id,
        source_code,
        stage,
        LISTAGG(variant, ', ') AS variants
    FROM random_num_table AS n
    LEFT JOIN {{ audience_schema }}.{{ brand_code }}_audience_variant_split AS a
        ON (split_from <= random_num AND random_num < split_to)
    GROUP BY
        random_num,
        source_id,
        source_code,
        stage
    HAVING
        SUM(
            CASE
                WHEN valid_until IS NULL THEN 1
                ELSE 0
            END
        ) > 1
;
