# Playvox Evaluations

|Column | Type | Description|
| :--- | :--- | :--- | 
|evaluation_id | STRING | Unique identifier of the Evaluation|
|site_id | STRING | Playvox instance (site) id|
|agent_id | STRING | Agent Playvox ID (that was being valuated)|
|cc_department | STRING | department (Customer, Partner or Rider Service) from the scorecard description|
|cc_team | STRING | team (Chat, Coms...) from the scorecard description|
|created_at | TIMESTAMP | Timestamp of the creation of the evaluation|
|evaluations | INTEGER | Number of evaluations performed|
|signed_evaluations | INTEGER | Number of evaluations signed by the agent (accepted)|
|score_avg | FLOAT | Avg score as a result of the evaluation|
|total_errors | INTEGER | Number of errors during the evaluation|
|monitor_id | STRING | ID of the Auditor who was leading the evaluation|
|scorecard | STRING | Scorecard ID related to the evaluatiion|
|team_description | STRING | Description of the team to which the Agent being evaluated belongs|
|reference | STRING | Case, Chat, Ticket, URL being the topic of the evalautions|
|contact_center | STRING | Contact Center|
|region | STRING | Region|
|management_entity | STRING | N/A|
|sections | STRING | N/A|