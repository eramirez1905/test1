CREATE TEMP FUNCTION parse_json(json STRING) -- function to return action components as an array
RETURNS ARRAY<STRUCT<type STRING, args ARRAY<STRUCT<name STRING, value STRING>>>>
LANGUAGE js AS """
  return JSON.parse(json).map((r) => {
    return {
      type: r.type,
      args: r.args.map((arg) => { return { name: arg.name, value: arg.value } } )
    }
  });
"""
;
CREATE TEMP FUNCTION parse_trigger_json(json STRING) -- function to return action trigger components as an array
RETURNS STRUCT<type STRING, args ARRAY<STRUCT<name STRING, value STRING>>>
LANGUAGE js AS """
  let r = JSON.parse(json)

  return {
    type: r.type,
    args: r.args.map((arg) => { return { name: arg.name, value: arg.value } } )
  }
"""
;
