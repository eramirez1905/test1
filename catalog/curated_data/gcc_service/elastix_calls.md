# Elastix Calls

|Column | Type | Description|
| :--- | :--- | :--- | 
|source_system | STRING | Elastix|
|call_id | STRING | unique call id|
|direction | STRING | inbound or outbound|
|call_timestamp | TIMESTAMP | timestamp of call start in Local Time|
|country | STRING | country|
|brand | STRING | brand|
|serving_language | STRING | language in which the call was handled|
|department | STRING | CS/PS/RS|
|department_id | INTEGER | 1,2,4 for CS, PS, RS|
|center | STRING | contact or dispatch center|
|region | STRING | region|
|management_entity | STRING | management entity (Talabat)|
|global_entity_id | STRING | global standard id of the entity (e.g TA_AE)|
|agent_id | STRING | Agent's ID in Elastix|
|agent_email | STRING | Agent's email|
|caller_id | STRING | phone number from the stakeholder (inbound)|
|dest_number | STRING | phone number from the stakeholder (outbound)|
|calls_inbound | INTEGER | 1 for inbound calls|
|calls_outbound | INTEGER | 1 for outbound calls|
|successful_outbound_call | INTEGER | 1 if the outbound call has the status Answered|
|inbound_served | INTEGER | 1 for inbound calls|
|inbound_invalid | INTEGER | 0 - Elastix does not record invalid or missed inb calls|
|inbound_missed | INTEGER | 0 - Elastix does not record invalid or missed inb calls|
|inbound_ht | INTEGER | handling time of the inbound call|
|outbound_ht | INTEGER | handling time of the outbound call|
|outbound_ht_less_3_min | INTEGER | 1 if the outbound call has a handling time of less than 3 min|
|inbound_qt_served | INTEGER | queue time of the inbound served calls|
|inbound_qt_missed | INTEGER | 0 - Elastix does not record invalid or missed inb calls|
|missed_qt_more_30_sec | INTEGER | 0 - Elastix does not record invalid or missed inb calls|
|inbound_within_sla_5 | INTEGER | 1 if the inbound call has a queue time of less than 5 seconds|
|inbound_within_sla_30 | INTEGER | 1 if the inbound call has a queue time of less than 30 seconds|
|inbound_within_sla_50 | INTEGER | 1 if the inbound call has a queue time of less than 50 seconds|
|inbound_within_sla_60 | INTEGER | 1 if the inbound call has a queue time of less than 60 seconds|