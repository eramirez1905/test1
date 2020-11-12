{% if environment == 'production' %}
CREATE OR REPLACE FUNCTION {{ audience_schema }}.f_audience_to_jsonl(external_id VARCHAR, ds_audience_name VARCHAR, ds_channel_control_group VARCHAR)
    RETURNS VARCHAR
IMMUTABLE
/*
    Generates JSONL to be consumed by Braze uploader

    This UDF is intended to be used in conjunction with UNLOAD job to generate a
    syntactiaclly correct JSONL document.
*/
AS $$
    import json
    if not external_id:
    	raise ValueError('"external_id" not provided or None when calling Python UDF f_audience_to_jsonl')
    return json.dumps(
        {
            'external_id': external_id,
            'ds_audience_name': ds_audience_name or "",
            'ds_channel_control_group': ds_channel_control_group or ""
        }
    )
$$ LANGUAGE PLPYTHONU
;

GRANT ALL PRIVILEGES ON FUNCTION {{ audience_schema }}.f_audience_to_jsonl(external_id VARCHAR, ds_audience_name VARCHAR, ds_channel_control_group VARCHAR) TO GROUP "mkt_tech";
GRANT ALL PRIVILEGES ON FUNCTION {{ audience_schema }}.f_audience_to_jsonl(external_id VARCHAR, ds_audience_name VARCHAR, ds_channel_control_group VARCHAR) TO GROUP "mkt_prd";
GRANT EXECUTE ON FUNCTION {{ audience_schema }}.f_audience_to_jsonl(external_id VARCHAR, ds_audience_name VARCHAR, ds_channel_control_group VARCHAR) TO GROUP "mkt_qa";
{% else %}
-- Python UDFs cannot be created by the qa user
-- which should be used to run in development
SELECT TRUE;
{% endif %}
