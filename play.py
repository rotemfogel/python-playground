import os
from typing import Optional

health_checks = [
    {
        'task_id': 'all_nodes_up',
        'check_type': 'exact',
        'query': "select count(*) as value from nodes where node_state <> 'UP'",
        'value': 0,
    },
    {
        'task_id': 'max_ros_count',
        'check_type': 'range',
        'query': "select max(ros_count) as value from projection_storage",
        'value': 750,
    },
    {
        'task_id': 'delete_vectors',
        'check_type': 'range',
        'query': "select nvl(max(del_cnt),0) as value from (select projection_name, sum(deleted_row_count) as del_cnt from delete_vectors group by 1) as res",
        'value': 20000000,
    },
    {
        'task_id': 'active_events',
        'check_type': 'exact',
        'query': "select count(*) as value from active_events where event_severity not in ('Informational','Notice')",
        'value': 0,
    },
    {
        'task_id': 'ahm_delay',
        'check_type': 'range',
        'query': "select nvl(max(del_cnt),0) as value from (select projection_name, sum(deleted_row_count) as del_cnt from delete_vectors group by 1) as res",
        'value': 100,
    },
    {
        'task_id': 'ahm_delay',
        'check_type': 'range',
        'query': "select get_current_epoch() - get_ahm_epoch() as value",
        'value': 100,
    },
    {
        'task_id': 'lge_delay',
        'check_type': 'range',
        'query': "select get_current_epoch() - get_ahm_epoch() as value",
        'value': 100,
    },
    {
        'task_id': 'projections_up_to_date',
        'check_type': 'exact',
        'query': "select count(*) from projections where not is_up_to_date",
        'value': 0,
    },
]


def _health_checks():
    for health_check in _health_checks:
        task_id = health_check['task_id']
        check_type = health_check['check_type']
        query = health_check['query']
        value = health_check['value']
        print(task_id, check_type, query, value)


tables_to_monitor = {
    "active_users": {
        "column": "created_at",
        "condition": "date_>=(sysdate-2)"
    },
    "authors": {
        "column": "created_on"
    },
    "comments": {
        "column": "created_on"
    },
    "content_tags": {
        "column": "updated_on",
        "condition": "updated_on<=(sysdate+1)"
    },
    "contents_agg": {
        "column": "created_at"
    },
    "email_events": {
        "column": "send_time",
        "condition": "date_>=(sysdate-2)",
        "database": "db_layer"
    },
    "marketing_attribution": {
        "column": "sub_date_time"
    },
    "page_events": {
        "column": "ts",
        "condition": "date_>=(sysdate-2)",
        "database": "db_layer"
    },
    "paying_subscriptions": {
        "column": "account_createddate",
        "condition": "evaluation_date>=(sysdate-2)"
    },
    "paying_users": {
        "column": "account_created_date"
    },
    "portfolios": {
        "column": "created_on"
    },
    "session": {
        "column": "session_start",
        "target": 26
    },
    "sessions_ab_test": {
        "column": "session_end"
    },
    "subscription_events": {
        "column": "ts",
        "condition": "date_>=(sysdate-2)"
    },
    "user_author_research_discounts": {
        "column": "created_at"
    },
    "user_followings": {
        "column": "created_on"
    },
    "user_registration_trackings": {
        "column": "registered"
    },
    "users": {
        "column": "created_at"
    },
    "visitors_aggregation": {
        "column": "date_"
    },
    "zuora_subscriptions": {
        "column": "subscription_start_date"
    }
}

default_db_name = 'db_layer'
schema = {
    'active_campaign': 'test_active_campaign',
    'db_adhoc': 'test_dba',
    'db_backup': 'test_dbb',
    'db_layer': 'test_dbl',
    'db_model': 'test_dbm',
    'db_raw_data': 'test_dbraw',
    'db_report': 'test_dbr',
    'db_test': 'test_dbi',
    'for_sapi': 'test_for_sapi',
    'mariadb': 'test_mariadb',
    'mariadb_scd': 'test_mariadb_scd',
    'staging': 'test_staging',
    'zuora': 'test_zuora'
}


def _main():
    for item in tables_to_monitor.items():
        table: str = item[0]
        table_config: dict = item[1]
        if not table_config:
            table_config = {table: {'column': 'date_'}}
        table_schema = schema[table_config.get('database', default_db_name)]
        delay = table_config.get('target', 3)
        query = f"SELECT MAX({table_config['column']}) FROM {table_schema}.{table}"
        if table_config.get('condition'):
            query = query + f" WHERE {table_config['condition']}"
        print(f'{table}:\t{query}')


sql_templates_dir = '/home/rotem/dev/workspace/seekingalpha/data-management-airflow/dags/sql_template/'


def _should_remove_prefix(template_path: str) -> bool:
    if len(template_path.split('_')) == 1:
        return False
    path = f'{sql_templates_dir}/athena/query/'
    sub_folders = [f.path.replace(path, '') for f in os.scandir(path) if f.is_dir()]
    return template_path not in sub_folders


def _build_path(template_path: Optional[str]):
    """
    build a path for sql_template

    method should yield
    '' => /
    dbr => dbr/
    for_sapi => for_sapi/
    test_for_sapi => for_sapi/
    poc_for_sapi => for_sapi/

    :returns path of type string
    :param template_path
    :type template_path: str
    """
    path = template_path if template_path else ''
    if template_path and len(template_path) > 0:
        templates = template_path.split('/')
        db = templates.pop(0)
        if _should_remove_prefix(db):
            paths = db.split('_')
            paths.pop(0)
            path = '_'.join(paths)
        if not path == template_path:
            if templates and templates[0]:
                path = '/'.join([path] + templates)
    if not path.endswith('/'):
        path = path + '/'
    return path


def _assert(input_db: str, expected_db: str = None) -> None:
    expected = expected_db + '/' if expected_db else input_db
    print(f'[{input_db}] == [{expected}]')
    assert (input_db == expected)


if __name__ == '__main__':
    _assert(_build_path(None))
    _assert(_build_path(''))
    _assert(_build_path('dbr'))
    _assert(_build_path('active_campaign'))
    _assert(_build_path('test_dbr'), 'dbr')
    _assert(_build_path('poc_dbr'), 'dbr')
    _assert(_build_path('for_sapi'))
    _assert(_build_path('test_for_sapi'), 'for_sapi')
    _assert(_build_path('poc_for_sapi'), 'for_sapi')
    _assert(_build_path('dbr/'))
    _assert(_build_path('test_dbr/'), 'dbr')
    _assert(_build_path('poc_dbr/'), 'dbr')
    _assert(_build_path('for_sapi/'))
    _assert(_build_path('test_for_sapi/'), 'for_sapi')
    _assert(_build_path('poc_for_sapi/'), 'for_sapi')
    _assert(_build_path('dbr/mp_kpi_dash_interim'), 'dbr/mp_kpi_dash_interim')
    _assert(_build_path('test_dbr/mp_kpi_dash_interim'), 'dbr/mp_kpi_dash_interim')
    _assert(_build_path('poc_dbr/mp_kpi_dash_interim'), 'dbr/mp_kpi_dash_interim')
    _assert(_build_path('for_sapi/mp_kpi_dash_interim'), 'for_sapi/mp_kpi_dash_interim')
    _assert(_build_path('test_for_sapi/mp_kpi_dash_interim'), 'for_sapi/mp_kpi_dash_interim')
    _assert(_build_path('poc_for_sapi/mp_kpi_dash_interim'), 'for_sapi/mp_kpi_dash_interim')
