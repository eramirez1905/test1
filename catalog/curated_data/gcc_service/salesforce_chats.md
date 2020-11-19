# Salesforce Chats

|Column | Type | Description|
| :--- | :--- | :--- | 
|contact_center | STRING | Contact Center|
|region | STRING | Region|
|global_entity_id | STRING | N/A|
|management_entity | STRING | N/A|
|country | STRING | Country|
|language | STRING | Language|
|brand | STRING | Brand|
|chat_name | STRING | N/A|
|chat_button | STRING | Chat button on which the user clciks ,Determines the Language and the Department of the chat (e.g. CA_ARA). Developer name|
|chat_id | STRING | Unique identifier of the Chat in Salesforce|
|agent_id | STRING | Agent who served the chat|
|chat_status | STRING | Status of the Chat (completed, Missed)|
|chat_timestamp | TIMESTAMP | Timestamp at which the chat was initiated by the customer|
|department_id | INTEGER | department id (1 = Customer, 2 = Partner or 4 = Rider Service)|
|incoming_chats | INTEGER | N/A|
|work_items | INTEGER | Work Items for CS/PS Chats|
|queue_time | INTEGER | Queue time of the chat|
|messages_visitor | INTEGER | Number of messages sent by the visitor|
|messages_agents | INTEGER | Number of messages sent by the agent|
|messages_total | INTEGER | Total number of messages exchanged|
|operating_system | STRING | operating system of the visitor|
|missed_chats | INTEGER | Number of chats with status Missed|
|served_chats | INTEGER | Number of chats with status Completed|
|ht_served | INTEGER | Handling tiime of Served Chats|
|ht_missed | INTEGER | Handling tiime of Missed Chats|
|response_time_avg | INTEGER | Average response time between 2 messages|
|response_time_max | INTEGER | Maximum response time between 2 messages|
|chat_within_sla_5 | INTEGER | Chat within SLA 5 sec (First Reply happened in less than 5 sec)|
|chat_within_sla_10 | INTEGER | Chat within SLA 10 sec (First Reply happened in less than 10 sec)|
|chat_within_sla_30 | INTEGER | Chat within SLA 30 sec (First Reply happened in less than 30 sec)|
|chat_within_sla_50 | INTEGER | Chat within SLA 50 sec (First Reply happened in less than 50 sec)|
|chat_within_sla_60 | INTEGER | Chat within SLA 60 sec (First Reply happened in less than 60 sec)|
|case_id | STRING | Related case ID (if the chat created a chat case)|
|customer_email | STRING | Customer Email|
|alan_identifier | STRING | Related unique Help Center / Alan identifier in case this chat originated from the helpcenter|
|ccr1 | STRING | Customer/Partner Contact Reason Level 1|
|ccr2 | STRING | Customer/Partner Contact Reason Level 2|
|ccr3 | STRING | Customer/Partner Contact Reason Level 3|