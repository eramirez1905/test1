# Avaya Chats

|Column | Type | Description|
| :--- | :--- | :--- | 
|chat_id | STRING | unique identifier of the chat in Avaya system|
|contact_center | STRING | default to Turkey|
|region | STRING | N/A|
|global_entity_id | STRING | YS_TR|
|management_entity | STRING | default to Yemeksepeti|
|country | STRING | default to Turkey|
|brand | STRING | default to Yemeksepeti|
|language | STRING | English/Turkish|
|department_code | INTEGER | default to 1 for Customer Service|
|agent_chat_id | INTEGER | agent id in avaya system for chats|
|agent_email | STRING | email of the agent that served the chat (loginid@yemeksepeti.com)|
|manager_email | STRING | email of the Team Leader of the agent that served the chat|
|chat_status | STRING | Completed or Missed|
|created_timestamp | TIMESTAMP | timestamp of the chat creation in local time|
|handling_time | INTEGER | time in seconds that the agent spent serving the chat|
|ht_served | INTEGER | handling time in case of a served chat|
|ht_missed | INTEGER | handling time in case of a missed chat|
|queue_time | INTEGER | queue time of the chat|
|incoming_chats | INTEGER | N/A|
|missed_chats | INTEGER | 1 when the incoming chat is missed (agent id is null) else 0|
|served_chats | INTEGER | 1 when the incoming chat is served (agent id is not null) else 0|
|work_items | INTEGER | 1 if chat is served, or missed after at least 30 sec of queue time|
|first_reply_time | INTEGER | if chat is served, queue time of the chat (proxy)|
|chat_within_sla_5 | INTEGER | 1 if first_reply_time <5 sec|
|chat_within_sla_10 | INTEGER | 1 if first_reply_time <10 sec|
|chat_within_sla_30 | INTEGER | 1 if first_reply_time <30 sec|
|chat_within_sla_50 | INTEGER | 1 if first_reply_time <50 sec|
|chat_within_sla_60 | INTEGER | 1 if first_reply_time <60 sec|
|helpcenterchatid | STRING | helpcenter identifer of the session from which the chat originated|