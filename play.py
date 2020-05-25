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

for health_check in health_checks:
    task_id = health_check['task_id']
    check_type = health_check['check_type']
    query = health_check['query']
    value = health_check['value']
    print(task_id, check_type, query, value)
