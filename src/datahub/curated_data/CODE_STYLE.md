# Code Style

This guide defines the BigQuery coding standards we use at DeliveryHero. 
The intent of this guide is to help developers create BigQuery queries that are readable, portable, easy to maintain and bug-free.

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" 
in this document are to be interpreted as described in [RFC 2119](https://www.ietf.org/rfc/rfc2119.txt).

## General

- All BigQuery SQL reserved words MUST be written in upper case.
- All BigQuery SQL MUST have the `.sql` extension.
- File names MUST only contain letters (a-z) and underscore`_`.
- All indents MUST be done with 2 white spaces, MUST NOT be done with `tabs`.
- A file MUST NOT end with `;`, as BigQuery doesn't allow to run 2 queries in the same file.
- A file MUST end with a new line, and only one.

### Limits

All lines SHOULD NOT exceed 120 characters, including whitespaces. If a line is broken into two, the subsequent MUST be indented with 2 white spaces.

The following condition:

```sql
    AND (GREATEST(COALESCE(TIMESTAMP_DIFF(rider_picked_up_at, rider_near_restaurant_at, SECOND), 0), 0) + COALESCE(TIMESTAMP_DIFF(rider_near_restaurant_at, o.original_scheduled_pickup_at, SECOND), 0)) < 0
```

Is broken into:

```sql
    AND (GREATEST(COALESCE(TIMESTAMP_DIFF(rider_picked_up_at, rider_near_restaurant_at, SECOND), 0), 0) 
          + COALESCE(TIMESTAMP_DIFF(rider_near_restaurant_at, o.original_scheduled_pickup_at, SECOND), 0)) < 0
```

## General Structure

- If the query is short (less than 60 characters), it MAY be written as a single line.

```SQL
    SELECT * FROM temp.foo WHERE created_date = '2019-09-09'
```

- Otherwise, each statement MUST be written in separate lines.
- Each statement MUST have the same indentation level.

```SQL
    SELECT *
    FROM temp.foo
    WHERE created_date = '2019-09-09'
    GROUP BY country_code, zone_id
    ORDER BY created_at
``` 

## Select Statement
 
- From the second attribute onwards of a `SELECT` statement, each attribute MUST have a leading comma. 
- Each leading comma MUST be indented by two spaces in respect to the `SELECT` statement.

```SQL
    SELECT country_code
      , zone_id
      , id
```

### ARRAY_AGG + STRUCT 

When aggregating into an array of structs, the following guidelines should be followed:

- The opening parenthesis of the `ARRAY_AGG` MUST be on the same line as the `ARRAY_AGG` keyword.
- The `STRUCT` MUST be in a new line with two spaces of indent in respect to `ARRAY_AGG`.
- The opening parenthesis of the `STRUCT` MUST be on the same line as the `STRUCT` keyword.
- The first element of the `STRUCT` SHOULD be on the same line as `STRUCT` or SHOULD be in a new line indented by 2 spaces in respect to `STRUCT`.
- All columns in the `STRUCT` MUST have leading commas.
- The closing parenthesis of `STRUCT` MUST be in the same level and the `STRUCT` keyword.
- The closing parenthesis of `ARRAY_AGG` MUST be in the same level and the `ARRAY_AGG` keyword.
- The aggregation `ARRAY_AGG` SHOULD be given an alias.

```sql
    SELECT a.country_code
      , ARRAY_AGG(
          STRUCT(a.id
            , a.start_at
            , a.end_at
          )
        ) AS absences_history
    FROM temp.foo a
```

Or,

```sql
    SELECT a.country_code
      , ARRAY_AGG(
          STRUCT(
            a.id
            , a.start_at
            , a.end_at
          )
        ) AS absences_history
    FROM temp.foo a
```

## WHERE statement

- The first condition of the `WHERE` statement MUST be written on the same line.
- From the second condition onwards, they MUST BE written on a new line with two spaces of indent in respective to `WHERE`.

```sql
    WHERE country_code = 'ca'
      AND zone_id = 7
      AND region = 'America'
```

## LEFT JOIN Statement

- The first condition of the `LEFT JOIN` statement MUST be written on the same line
- From the second condition onwards, they MUST BE written on a new line with two spaces of indent in respective to `WHERE`.
- You MAY give the joined table an alias.

```sql
    FROM porygon_events_version pv
    LEFT JOIN porygon_events pe ON country_code = 'ca'
      AND zone_id = 7 
      AND region = 'America'
```

## GROUP BY statement

In general, we prefer using the numerical approach when specifying the `GROUP BY` statement. This allows us to keep it in a single line.

- You SHOULD write the `ORDER BY` statement using ordinal positions. 

```sql
    SELECT country_code
      , zone_id
    FROM events
    GROUP BY 1, 2
```

- You MAY write the `ORDER BY` statement using column names.

```sql
    SELECT country_code
      , zone_id
    FROM events
    GROUP BY country_code, zone_id
```

## WITH statement

- The subquery name MUST be in the same line as the `WITH` keyword.
- The `AS` keyword MUST be in the same line as the `WITH` keyword.
- The opening parenthesis of the subquery MUST be in the same line as the `WITH` keyword.
- All statements of the subquery MUST be indented by two space in respect to the `WITH` statement.
- The closing parenthesis MUST be at the same indentation level as the `WITH` keyword. 
- Any subsequent subquery name, `AS` keyword and opening parenthesis MUST be in the same line as the previous closing parenthesis.
- The query indent level MUST match the `WITH` keyword.

```sql
WITH foo AS (
    SELECT *
    FROM poryon
    WHERE action = 'close' 
), bar AS (
    SELECT *
    FROM porygon_versions
    WHERE action = 'close'
)
SELECT *
FROM foo f
LEFT JOIN bar b ON b.country_code = f.country_code 
```

## CASE statement 

- If the `CASE` statement is short (less than 80 characters), it MAY be written in a single line,

```sql
    CASE WHEN action = 'lockdown' THEN 'close' ELSE action END AS event_type
```

- Notice that if the `CASE` has only one condition, it MAY be writen as an `IF` statement:

```sql
    IF(action = 'lockdown', 'close', action) AS event_type
```

- Moreover, if the condition returns a boolean, it SHOULD be written without the `IF`

```sql
   action IS NOT NULL AS has_action 
```

Otherwise, when the condition has more than one condition, the following guidelines must be followed:
 
- The `WHEN` keyword MUST be written in a new line indented by two spaces in respect to the `CASE`.
- All conditions MUST be written in the same line as the `WHEN` keyword. 
- The `THEN` keyword MUST be written in a new line with two spaces of indentation in respect to `WHEN`.
- All new `WHEN` statements MUST have the same indentation of the original `WHEN`.
- The `ELSE` keyword MUST be at the same indentation level of `WHEN`.
- The `END` keyword MUST be at the same indentation level of `CASE`.

```sql
    CASE 
      WHEN action = 'lockdown' AND created_date = '2019-09-09' AND country = 'ca'
        THEN 'close'
      WHEN action = 'shrink' AND created_date = '2019-09-09' AND country = 'ph'
        THEN 'close'
      ELSE action
    END AS event_type
``` 

- The `WHEN` conditions SHOULD NOT exceed the 120 characters, instead consider using subqueries.

In the example below, notice `TIMESTAMP_DIFF(rider_near_restaurant_at, o.original_scheduled_pickup_at, SECOND)` is repeated several times. 
This is an anti pattern for performance, as this operation SHOULD be done only once in a subquery. 

```sql
    CASE
      WHEN auto_transition.pickup IS NULL AND auto_transition.dropoff IS NULL AND TIMESTAMP_DIFF(rider_near_restaurant_at, o.original_scheduled_pickup_at, SECOND) <= 0
        THEN TIMESTAMP_DIFF(rider_dropped_off_at, rider_picked_up_at, SECOND)
      WHEN auto_transition.pickup IS NULL AND auto_transition.dropoff IS NULL AND TIMESTAMP_DIFF(rider_near_restaurant_at, o.original_scheduled_pickup_at, SECOND) > 0 AND TIMESTAMP_DIFF(rider_picked_up_at, rider_near_restaurant_at, SECOND) < 300
        THEN TIMESTAMP_DIFF(rider_dropped_off_at, o.updated_scheduled_pickup_at, SECOND)
      WHEN auto_transition.pickup IS NULL AND auto_transition.dropoff IS NULL AND TIMESTAMP_DIFF(rider_near_restaurant_at, o.original_scheduled_pickup_at, SECOND) > 0 AND TIMESTAMP_DIFF(rider_picked_up_at, rider_near_restaurant_at, SECOND) >= 300
        THEN TIMESTAMP_DIFF(rider_dropped_off_at, rider_picked_up_at, SECOND)
      ELSE NULL
    END AS foo    
```

## UNION + Subqueries

- A new line SHOULD be used between `UNION` subqueries.

```sql
    SELECT *
    FROM foo
    
    UNION
    
    SELECT *
    FROM bar
```

## CREATE statement

- The `CREATE`, `PARTITION BY`, `CLUSTER BY` statements MUST be written into new lines without indentation.  

```sql
    CREATE OR REPLACE TABLE temp.foo
    PARTITION BY created_date
    CLUSTER BY country_code, zone_id, id AS
``` 

- The `CLUSTER BY` statement MUST NOT have more than 4 columns. For further information, read [here](https://cloud.google.com/bigquery/docs/creating-clustered-tables#limitations)

## Dont's

### Subquery in FROM statement

The following structure MUST NOT be used:

```sql
    SELECT *
    FROM (
        SELECT delivery_id
          , timezone
        FROM temp.foo
        WHERE delivery_id IS NOT NULL
      )
```

## Column Conventions

### Dates and timestamps

- Date columns MUST end with the suffix `_date`.
- Timestamp columns MUST end with the suffix `_at`.
- To get the `current_date` or `current_timestamp`, you MUST use the respective airflow macros `'{{ next_ds }}'` or `'{{ next_execution_date }}'` respectively. For more references on airflow macros, please read [here](https://airflow.apache.org/macros.html)

### Booleans

- Boolean columns MUST have an appropriate prefix: `is_`, `has_`. Examples: `is_shape_in_sync` or `has_timezone`. 
