# Playvox Evaluations Questions

|Column | Type | Description|
| :--- | :--- | :--- | 
|scorecard_name | STRING | Scorecard Name related to the evaluatiion|
|site_id | STRING | Playvox instance (site) id|
|scorecard_id | STRING | Scorecard ID related to the evaluatiion|
|evaluation_id | STRING | Unique identifier of the Evaluation|
|agent_id | STRING | Agent Playvox ID (that was being evaluated)|
|question_id | STRING | Unique identifier of the Question|
|contact_center | STRING | Contact Center|
|region | STRING | Region|
|management_entity | STRING | Management Entity|
|agent | STRING | Agent Name (that was being evaluated)|
|email | STRING | Agent Email (that was being evaluated)|
|user_name | STRING | Agent Playvox Username (that was being evaluated)|
|team | STRING | Agent's Team name|
|monitor | STRING | Name of the Auditor|
|points | FLOAT | Points achieved to the question|
|max_score | FLOAT | Maximum score achiveved to the question|
|qa_score | FLOAT | QA Score|
|goal | FLOAT | Target Score|
|achieved | STRING | Yes if target was achieved|
|date_created | DATE | date at which the evaluation was created|
|signed | STRING | Yes if the agent accepted the audit|
|feedback | STRING | N/A|
|reference | STRING | Case, Chat, Ticket, URL being the topic of the evaluations|
|section_order | INTEGER | ordinal of the section|
|section | STRING | section's name|
|score_section | FLOAT | score of the section|
|max_score_section | FLOAT | maiximum score of the section|
|percentage_section | FLOAT | N/A|
|section_affected_by | STRING | N/A|
|feedback_section | STRING | N/A|
|question_order | INTEGER | ordinal of the question|
|question | STRING | question's name|
|answer | STRING | answer to the question|
|score_answer | FLOAT | score of the answer|
|max_score_answer | FLOAT | maximum score of the answer|
|answer_type | STRING | type of the answer (default, ...)|
|comment_question | STRING | comment attached|