from pprint import pprint


def is_production() -> bool:
    return True


class DatabaseType:
    VERTICA = "vertica"
    ATHENA = "athena"


report = is_production()
configuration = [
    {
        "task_id": "mobile_apps_paywall_b_click_to_shown_ratio",
        "name": "Mobile Apps - Paywall B Ratio Click/Shown ratio",
        "operator": 5,  # GREATER_THAN_OR_EQUAL
        "threshold": 50,
        "source_db": DatabaseType.VERTICA,
        "query": "mpw_paywall_ratio.sql",
        "report_to_slack": report,
        "slack_channel": "#piano-notifications",
        "report_to_pagerduty": report,
        "pagerduty_key": "mpw",
        "params": {
            "paywall_groups": ["B"],
            "client_type": ["Mobile Apps - Apple", "Mobile Apps - Android"],
            "join_condition": "b.referrer_key=a.page_key",
        },
    },
    {
        "task_id": "mobile_web_paywall_b_click_to_shown_ratio",
        "name": "Mobile Web - Paywall B Ratio Click/Shown ratio",
        "operator": 5,  # GREATER_THAN_OR_EQUAL
        "threshold": 50,
        "source_db": DatabaseType.VERTICA,
        "query": "mpw_paywall_ratio.sql",
        "report_to_slack": report,
        "slack_channel": "#piano-notifications",
        "report_to_pagerduty": report,
        "pagerduty_key": "mpw",
        "params": {
            "paywall_groups": ["B", "C"],
            "client_type": ["Mobile Web"],
            "join_condition": "b.session_cookie_key_int=a.session_cookie_key_int",
        },
    },
    {
        "task_id": "desktop_paywall_b_click_to_shown_ratio",
        "name": "Desktop - Paywall B Ratio Click/Shown ratio",
        "operator": 5,  # GREATER_THAN_OR_EQUAL
        "threshold": 50,
        "source_db": DatabaseType.VERTICA,
        "query": "mpw_paywall_ratio.sql",
        "report_to_slack": report,
        "slack_channel": "#piano-notifications",
        "report_to_pagerduty": report,
        "pagerduty_key": "mpw",
        "params": {
            "paywall_groups": ["B"],
            "client_type": ["Desktop"],
            "join_condition": "b.referrer_key=a.page_key",
        },
    },
    {
        "task_id": "page_events_records_ratio",
        "name": "Page Events writes ratio",
        "operator": 5,  # GREATER_THAN_OR_EQUAL
        "threshold": 65,
        "source_db": DatabaseType.ATHENA,
        "query": "page_events_records_ratio.sql",
        "report_to_slack": report,
        "slack_channel": "#data-engineering",
        "report_to_pagerduty": report,
        "pagerduty_key": "data",
    },
    {
        "task_id": "mp_kpi_dashboard_alert",
        "name": "MP KPI Dashboard Is Empty",
        "operator": 4,  # GREATER_THAN
        "threshold": 0,
        "source_db": DatabaseType.ATHENA,
        "query": """
            SELECT COUNT(1)
              FROM {{ var.json.schema.for_sapi }}.mp_kpi_dashboard
             WHERE date_ = date '{{ macros.ds_ny(execution_date) }}'
            """,
        "report_to_slack": report,
        "slack_channel": "#data-engineering",
        "report_to_pagerduty": report,
        "pagerduty_key": "data",
        "params": {
            "error_message": ":no_entry: {{ var.json.schema.for_sapi }}.mp_kpi_dashboard is empty !\n"
            + "Environment: *{{ var.value.env }}*, Execution Date: {{ execution_date }}"
        },
    },
    {
        "task_id": "mp_kpi_no_null_fields",
        "name": "MP KPI Dashboard No Null Values",
        "operator": 2,  # LESS_THAN
        "threshold": 0,
        "source_db": DatabaseType.ATHENA,
        "query": """
            SELECT CASE WHEN COUNT(1) = 0 THEN -1 ELSE COUNT(1) END 
              FROM {{ var.json.schema.for_sapi }}.mp_kpi_dashboard
            {% for column in params.columns %} {{ "OR" if not loop.first else "WHERE" }} {{ column }} IS NULL
            {% endfor %}""",
        "report_to_slack": report,
        "slack_channel": "#data-engineering",
        "report_to_pagerduty": report,
        "pagerduty_key": "data",
        "params": {
            "error_message": ":no_entry: {{ var.json.schema.for_sapi }}.mp_kpi_dashboard has NULL values !\n"
            + "Environment: *{{ var.value.env }}*, Execution Date: {{ execution_date }}",
            # 'allow_free_trial' column is not included since it's null by default:
            # https://github.com/seekingalpha/data-management-airflow/blob/master/dags/sql_template/athena/query/for_sapi/mp_kpi_dash_interim/mp_kpi_dashboard.sql#L24
            "columns": [
                "product_id",
                "date_",
                "paying_subscriptions",
                "free_trial_subscriptions",
                "paying_subscriptions_pending_cancelation",
                "paying_annual_subscriptions",
                "paying_mrr",
                "checkout_conversion_rate_28day_ma",
                "free_trial_conversion_rate_28day_ma",
                "followers",
                "article_clicks_to_lp_28day_ma",
            ],
        },
    },
]

override_conf = {"page_events_records_ratio": {"threshold": -7}}

# override_conf = {'page_events_records_ratio': {'b': 1}}
if __name__ == "__main__":
    for config in configuration:
        task_id = config["task_id"]
        if override_conf:
            override_task = override_conf.get(task_id)
            if override_task:
                for key, val in override_task.items():
                    if config.get(key):
                        config.update({key: val})
    pprint(
        list(
            filter(lambda x: x["task_id"] == "page_events_records_ratio", configuration)
        )[-1]
    )
