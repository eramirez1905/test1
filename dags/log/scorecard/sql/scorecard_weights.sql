CREATE OR REPLACE TABLE rl.scorecard_weights AS
SELECT feature AS variable
  , weight
FROM rl.variable_importances_infra

UNION ALL

SELECT feature AS variable
  , weight
FROM rl.variable_importances_dispatching

UNION ALL

SELECT feature AS variable
  , weight
FROM rl.variable_importances_rider

UNION ALL

SELECT feature AS variable
  , weight
FROM rl.variable_importances_staffing

UNION ALL

SELECT feature AS variable
  , weight
FROM rl.variable_importances_vendor
