from pendulum import DateTime


# faster flat_map functions then using lambda
# See: https://gist.github.com/turbaszek/9edfa4f19a2b490eeddc8f78542de98c
def flat_map(f, xs):
    ys = []
    for x in xs:
        ys.extend(f(x))
    return ys


alert_tasks: dict = {'dags': {'emr_email_events': {'tasks': [{'copy': {}}]},
                              'emr_page_events': {'tasks': [{'partition_rollup_add_step': {}}]},
                              'paying_subscriptions': {'from': 5, 'to': 21},
                              'subscription_events': {'tasks': ['ccc', 'aaa']},
                              'user_registration_trackings': {},
                              'vertica_health_check': {'tasks': [{'all_nodes_up': {}},
                                                                 {'license_utilization': {'from': 5,
                                                                                          'to': 21}}]}},
                     'tasks': ['check_result', 'copy_qa', 'test_uniqueness']}
default_tasks = alert_tasks['tasks']


def report_to_pagerduty_with_conf(dag_id: str, task_id: str, execution_date: DateTime) -> bool:
    dags: dict = alert_tasks['dags']
    dag: dict = dags.get(dag_id, {dag_id: {}})
    dag_tasks = dag['tasks'] if dag.get('tasks') else default_tasks

    # allow override of tasks in dag level
    # e.g. {'dags': {'emr_page_events': {'tasks': ['copy', 'copy_qa']}}}
    execution_hour = execution_date.hour

    # check 'from' and 'to' hours for reporting, fallback to current hour
    default_hours = {'from': execution_hour, 'to': execution_hour}

    tasks_list = []
    try:
        if type(dag_tasks[0]) is dict:
            # join specific DAG tasks with the default tasks
            tasks_list = set(list(flat_map(lambda x: x.keys(), dag_tasks)) + default_tasks)
        elif type(dag_tasks[0]) is str:
            tasks_list = set(dag_tasks + default_tasks)
    except AttributeError:
        pass

    # make sure tasks_list is not empty
    if not tasks_list:
        tasks_list = default_tasks

    # if DAG has specific task, use them, otherwise fallback to default tasks
    report_hours = {}
    for dag_task in dag_tasks:
        if not report_hours and hasattr(dag_task, 'get') and dag_task.get(task_id):
            report_hours = dag_task[task_id]
    if not report_hours:
        report_hours = default_hours

    # if task_id name contains one of the configured task
    # for example: alert_tasks contains `copy_qa`
    #              user_registration_trackings.copy_qa should yield True
    #              since it contains the phrase `copy_qa`
    dag_ids: list = list(dags.keys())
    should_report = dag_id in dag_ids and task_id in tasks_list
    # execution hour is between 'from' and 'to'
    should_report = should_report and report_hours['from'] <= execution_hour <= report_hours['to']
    return should_report


if __name__ == "__main__":
    execution_date = DateTime.now()
    assert (report_to_pagerduty_with_conf(dag_id='user_registration_trackings', task_id='copy_qa',
                                          execution_date=execution_date))
    assert (report_to_pagerduty_with_conf(dag_id='subscription_events', task_id='ccc',
                                          execution_date=execution_date))
    assert (report_to_pagerduty_with_conf(dag_id='emr_page_events', task_id='partition_rollup_add_step',
                                          execution_date=execution_date))
    assert (report_to_pagerduty_with_conf(dag_id='vertica_health_check', task_id='all_nodes_up',
                                          execution_date=execution_date))
