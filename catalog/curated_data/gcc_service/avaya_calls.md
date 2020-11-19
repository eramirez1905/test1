# Avaya Calls

|Column | Type | Description|
| :--- | :--- | :--- | 
|source_system | STRING | default to Avaya|
|call_id | STRING | unique identifier of the call interaction in the Avaya system|
|direction | STRING | Inbound or Outbound|
|call_timestamp | TIMESTAMP | timestamp of the beginning of the call in local timezone (Turkey)|
|country | STRING | default to Turkey|
|brand | STRING | default to Yemeksepeti|
|serving_language | STRING | default to Turkish|
|department | STRING | Customer or Partner Service|
|department_id | INTEGER | 1 is Customer Service, 2 if Partner Service|
|center | STRING | default to Turkey|
|region | STRING | N/A|
|management_entity | STRING | default to Yemeksepeti|
|global_entity_id | STRING | YS_TR|
|agent_call_id | INTEGER | avaya inbound or outbound agent id|
|agent_email | STRING | email of the agent that served or placed the call|
|group_name | STRING | name of the group to which the agent belongs|
|calls_inbound | INTEGER | 1 if the call is inbound, else 0|
|calls_outbound | INTEGER | 1 if the call is outbound, else 0|
|successful_outbound_call | INTEGER | 1 if the call is outbound with duration >3 min|
|inbound_served | INTEGER | 1 if the call is inbound and served (agent id not null or disposition =2), else 0|
|inbound_invalid | INTEGER | default to 0|
|inbound_missed | INTEGER | 1 if the call is inbound and missed (agent id null), else 0|
|inbound_ht | INTEGER | time in seconds spent by the inbound agent to serve the call (talktime)|
|outbound_ht | INTEGER | time in seconds spent by the inbound agent to place the call (does not include ringing)|
|outbound_ht_less_3_min | INTEGER | 1 if the call is outbound and has HT less than 3 min, else 0|
|inbound_qt_served | INTEGER | for inbound served calls, time spent in the queue in seconds (queuetime)|
|inbound_qt_missed | INTEGER | for inbound missed calls, time spent in the queue in seconds (queuetime)|
|missed_qt_more_30_sec | INTEGER | 1 if the call is inbound and missed and has QT more than 30 sec, else 0|
|inbound_within_sla_5 | INTEGER | 1 if the call is inbound and has QT <5 sec, else 0|
|inbound_within_sla_30 | INTEGER | 1 if the call is inbound and has QT <30 sec, else 0|
|inbound_within_sla_50 | INTEGER | 1 if the call is inbound and has QT <50 sec, else 0|
|inbound_within_sla_60 | INTEGER | 1 if the call is inbound and has QT <60 sec, else 0|