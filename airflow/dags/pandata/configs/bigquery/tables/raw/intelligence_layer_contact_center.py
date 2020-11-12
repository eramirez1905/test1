from configs.constructors.table import BigQueryTable

INTELLIGENCE_LAYER_CCN_TABLES = (
    BigQueryTable(name="v_autocomp_fct_events"),
    BigQueryTable(name="v_autocomp_fct_triggers"),
    BigQueryTable(name="v_cc_self_reported_data"),
    BigQueryTable(name="v_dim_cc_countries"),
    BigQueryTable(name="v_dim_global_cc_users"),
    BigQueryTable(name="v_freshchat_fct_chats"),
    BigQueryTable(name="v_freshdesk_fct_tickets"),
    BigQueryTable(name="v_global_dim_ccr"),
    BigQueryTable(name="v_hc_fct_flow"),
    BigQueryTable(name="v_hc_fct_navigation_history"),
    BigQueryTable(name="v_hc_fct_session_events"),
    BigQueryTable(name="v_hc_fct_sessions"),
    BigQueryTable(name="v_intercom_fct_chats"),
    BigQueryTable(name="v_nvm_fct_calls"),
    BigQueryTable(name="v_pandacare_fct_chats"),
    BigQueryTable(name="v_pd_fct_ao_nps"),
    BigQueryTable(name="v_playvox_fct_calibrations"),
    BigQueryTable(name="v_playvox_fct_coachings"),
    BigQueryTable(name="v_playvox_fct_evaluation_customquestions"),
    BigQueryTable(name="v_playvox_fct_evaluation_questions"),
    BigQueryTable(name="v_playvox_fct_evaluations"),
    BigQueryTable(name="v_qualtrics_fct_ac_responses"),
    BigQueryTable(name="v_qualtrics_fct_csat_responses"),
    BigQueryTable(name="v_qualtrics_fct_no_contact_csat_responses"),
    BigQueryTable(name="v_rooster_fct_forecasted_incidents"),
    BigQueryTable(name="v_rooster_fct_shifts"),
    BigQueryTable(name="v_salesforce_fct_chats"),
    BigQueryTable(name="v_salesforce_fct_tickets"),
    BigQueryTable(name="v_salesforce_fct_work_items"),
    BigQueryTable(name="v_shyftplan_fct_absences"),
    BigQueryTable(name="v_shyftplan_fct_open_shift_blocks"),
    BigQueryTable(name="v_shyftplan_fct_shift_breaks"),
    BigQueryTable(name="v_shyftplan_fct_shifts"),
)
