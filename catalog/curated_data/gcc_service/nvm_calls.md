# Nvm Calls

|Column | Type | Description|
| :--- | :--- | :--- | 
|source_system | STRING | tool used to place the call (e.g. NVM)|
|call_id | STRING | Unique identifier of the Call|
|direction | STRING | Inbound or Outbound|
|call_timestamp | TIMESTAMP | Datetime of the start of the Call|
|agent_id | INTEGER | Agent who placed the call or Served the call|
|email | STRING | Email of the Agent who got assigned to the call|
|country | STRING | Country|
|brand | STRING | Brand|
|center | STRING | Contact / Dispatch Center|
|serving_language | STRING | N/A|
|main_language | STRING | N/A|
|department_id | INTEGER | department id (1 = Customer, 2 = Partner or 4 = Rider Service)|
|region | STRING | Region|
|management_entity | STRING | N/A|
|global_entity_id | STRING | N/A|
|calling_to | STRING | Country towards which the call is going|
|connect_from | STRING | Telephone number from which the call is placed|
|connect_to | STRING | Telephone number that the call is reaching towards|
|calls_inbound | INTEGER | Number of Inbound Calls|
|calls_outbound | INTEGER | Number of outbound Calls|
|successful_outbound_call | INTEGER | Number of Successful Outbound calls|
|inbound_served | INTEGER | Inbound Served calls|
|inbound_invalid | INTEGER | Inbound Calls Invalid|
|inbound_missed | INTEGER | Missed Inbound Calls|
|inbound_missed_online | INTEGER | Missed Inbound Calls during of working hours|
|inbound_missed_offline | INTEGER | Missed Inbound Calls outside of working hours|
|inbound_ht | FLOAT | Inbound Call Handling Time|
|inbound_after_call_time | FLOAT | Inbound Call Wrap Up Time after the call|
|outbound_ht | FLOAT | Outbound Call Handling Time|
|outbound_after_call_time | FLOAT | Outbound Call Wrap Up Time after the call|
|outbound_ht_less_3_min | INTEGER | Outbound call that lasted less than 3 min|
|inbound_qt_served | INTEGER | Queue Time of inbound calls that were served|
|inbound_qt_missed | INTEGER | Queue Time of inbound calls that were missed|
|missed_qt_more_30_sec | INTEGER | Missed inbound call with a queue time over 30 sec|
|inbound_within_sla_5 | INTEGER | Inbound call with SLA 5s (queue time less than 5s)|
|inbound_within_sla_30 | INTEGER | Inbound call with SLA 30s (queue time less than 30s)|
|inbound_within_sla_50 | INTEGER | Inbound call with SLA 50s (queue time less than 50s)|
|inbound_within_sla_60 | INTEGER | Inbound call with SLA 60s (queue time less than 60s)|
|work_items | INTEGER | Work Items Calls (inbound and outbound)|