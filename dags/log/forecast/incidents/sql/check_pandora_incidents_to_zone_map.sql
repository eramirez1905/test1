-- checking the mapping table sent from the Pandora DWH
SELECT
  COUNT(*) > 0 AS table_not_empty
FROM dl.pandora_rooster_incidents_teams_map
