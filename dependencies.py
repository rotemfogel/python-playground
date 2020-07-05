from datetime import datetime

from pendulum import Pendulum


def hourly_fn(execution_date: Pendulum) -> Pendulum:
    """
    default function for sensors of daily DAGs that depend on hourly DAGs
    assuming the daily task runs at 05:25
    :param execution_date: the DAG execution date
    :return: Pendulum
    """

    execution_date_transformed: Pendulum = execution_date.add(days=1)

    return execution_date_transformed


def hourly_one_earlier_fn(execution_date: Pendulum) -> Pendulum:
    """
    default function for sensors of daily DAGs that depend on hourly DAGs
    assuming the daily task runs at 05:25
    :param execution_date: the DAG execution date
    :return: Pendulum
    """

    execution_date_transformed: Pendulum = execution_date.add(hours=23)

    return execution_date_transformed


def daily_fn(execution_date: Pendulum) -> Pendulum:
    """
    default function for sensors of DAGs with a frequency larger then daily
    that depend on daily DAGs
    method accept execution date,
    extracts the hour from execution date
    and determines what date to pass the sensor,
    assuming it is a daily task (runs daily at 05:25).
    if execution_date hour < 5 then check 2 days days ago at 05:25
    otherwise check yesterday at 05:25
    example:
      for [2020-04-20 05:25:00] should be [2020-04-19 05:25:00]
      for [2020-04-20 16:25:00] should be [2020-04-19 05:25:00]
      for [2020-04-20 04:25:00] should be [2020-04-18 05:25:00]
    :param execution_date: the DAG execution date
    :return: Pendulum
    """
    hour: int = execution_date.hour
    closest_execution_date: Pendulum = execution_date.subtract(days=1).hour_(5).minute_(25).second_(0).microsecond_(0)
    if hour < 5:
        return closest_execution_date.subtract(days=1)
    return closest_execution_date


def _find_closest_hour(execution_date: Pendulum, hours: list) -> datetime:
    """
    method accept execution date,
    extracts the hour from execution date
    and finds the closest date to pass the sensor,
    assuming it is a recurring task based on list of hours.
    :param execution_date: the DAG execution date
    :type: Pendulum
    :param hours: the list of hours to match
    :type: array of numbers
    :return: Pendulum
    """
    hour = execution_date.hour

    # if arr exists in the array, return the execution date
    if hour in hours:
        return execution_date

    try:
        closest_hour = min([i for i in hours if i < hour], key=lambda x: abs(x - hour))
    # catch errors when min() functions accepts empty array
    # this happens when hour = 0 then i (index) = hour
    except ValueError:
        closest_hour = min(hours, key=lambda x: (abs(x - hour), x))

    if closest_hour > hour:
        return execution_date.subtract(hours=closest_hour)
    return execution_date.hour_(closest_hour)


def mariadb_fn(execution_date: Pendulum) -> datetime:
    """
    method accept execution date,
    and calls the _find_closest_hour with specific
    mariadb list of hours.
    :param execution_date: the DAG execution date
    :type: Pendulum
    :return: Pendulum
    example:
      2020-04-22T00:25:00+00:00 -> 2020-04-21T23:25:00+00:00
      2020-04-22T01:25:00+00:00 -> 2020-04-22T01:25:00+00:00
      2020-04-22T02:25:00+00:00 -> 2020-04-22T01:25:00+00:00
      2020-04-22T03:25:00+00:00 -> 2020-04-22T01:25:00+00:00
      2020-04-22T04:25:00+00:00 -> 2020-04-22T04:25:00+00:00
      2020-04-22T05:25:00+00:00 -> 2020-04-22T05:25:00+00:00
      2020-04-22T06:25:00+00:00 -> 2020-04-22T05:25:00+00:00
      ...
    """
    # array of hours depicting mariadb_dump runs
    arr = [1, 4, 5, 6, 9, 13, 17, 21]
    sensor_date = execution_date.minute_(25).second_(0).microsecond_(0)

    return _find_closest_hour(sensor_date, arr)


def ignore_past(execution_date: Pendulum) -> Pendulum:
    """
    Decide what execution_date the sensor should look for
    If execution date is more then 30 days ago, return execution date of 30 days ago
    otherwise return execution date
    :param execution_date:
    :return: Pendulum
    """

    execution_diff = execution_date.diff().in_days()

    if execution_diff > 30:
        return execution_date.add(days=(execution_diff - 30))

    return execution_date


def get_athena_dependencies(dependencies, params=None):
    """
    A function to set sensors based on dependencies provided

    Returns an array of sensors

    :param params:
    :param dependencies: A dict of schemas and tables
    :type dependencies: dic

    """

    sensors_config = {
        'maria_dim': {
            'task_id': 'maria_dim_sensor',
            'external_dag_id': 'emr_maria_dim',
            'external_task_id': 'end_of_dag',
            'poke_interval': 90,
            'mode': 'reschedule',
            'timeout': 3 * 60 * 60,
            'execution_date_fn': ignore_past,
        },
        'maria_dim_incr': {
            'task_id': 'maria_dim_incr_sensor',
            'external_dag_id': 'emr_maria_dim_incr',
            'external_task_id': 'end_of_dag',
            'poke_interval': 90,
            'mode': 'reschedule',
            'timeout': 2 * 60 * 60,  # although one should be enough according the previous runs
            'execution_date_fn': ignore_past,
        },
        'contents': {
            'task_id': 'contents_sensor',
            'external_dag_id': 'contents.athena_contents',
            'external_task_id': 'completed',
            'poke_interval': 90,
            'mode': 'reschedule',
            'timeout': 3 * 60 * 60,
            'execution_date_fn': ignore_past,
        },
        'contents_agg': {
            'task_id': 'contents_agg_sensor',
            'external_dag_id': 'contents_agg.athena_contents_agg',
            'external_task_id': 'completed',
            'poke_interval': 90,
            'mode': 'reschedule',
            'timeout': 3 * 60 * 60,
            'execution_date_fn': ignore_past,
        },
        'content_tags': {
            'task_id': 'content_tags_sensor',
            'external_dag_id': 'content_tags.athena_content_tags',
            'external_task_id': 'completed',
            'poke_interval': 90,
            'mode': 'reschedule',
            'timeout': 3 * 60 * 60,
            'execution_date_fn': ignore_past,
        },
        'sessions_ab_test_maria_dim': {
            'task_id': 'sessions_ab_test_maria_dim_sensor',
            'external_dag_id': 'athena_incr_sessions_ab_test.athena_sessions_ab_test_maria_dim_ds',
            'external_task_id': 'completed',
            'poke_interval': 90,
            'mode': 'reschedule',
            'timeout': 3 * 60 * 60,
            'execution_date_fn': ignore_past,
        },
        'sessions_ab_test_page_event': {
            'task_id': 'sessions_ab_test_maria_dim_sensor',
            'external_dag_id': 'athena_incr_sessions_ab_test.athena_sessions_ab_test_page_event_ds',
            'external_task_id': 'completed',
            'poke_interval': 90,
            'mode': 'reschedule',
            'timeout': 3 * 60 * 60,
            'execution_date_fn': ignore_past,
        },
        'authors': {
            'task_id': 'authors_sensor',
            'external_dag_id': 'authors.athena_authors',
            'external_task_id': 'completed',
            'poke_interval': 90,
            'mode': 'reschedule',
            'timeout': 3 * 60 * 60,
            'execution_date_fn': ignore_past,
        },
        'session': {
            'task_id': 'session_sensor',
            'external_dag_id': 'session_interim_tables',
            'external_task_id': 'add_partition',
            'poke_interval': 90,
            'mode': 'reschedule',
            'timeout': 3 * 60 * 60,
            'execution_date_fn': ignore_past,
        },
        'user_followings': {
            'task_id': 'user_followings_sensor',
            'external_dag_id': 'user_followings.athena_user_followings',
            'external_task_id': 'completed',
            'poke_interval': 90,
            'mode': 'reschedule',
            'timeout': 3 * 60 * 60,
            'execution_date_fn': ignore_past,
        },
        'tickers': {
            'task_id': 'tickers_sensor',
            'external_dag_id': 'tickers',
            'external_task_id': 'athena_tickers',
            'poke_interval': 90,
            'mode': 'reschedule',
            'timeout': 3 * 60 * 60,
            'execution_date_fn': ignore_past,
        },
        'content_disclosures': {
            'task_id': 'content_disclosures_sensor',
            'external_dag_id': 'content_disclosures',
            'external_task_id': 'end_of_dag',
            'poke_interval': 90,
            'mode': 'reschedule',
            'timeout': 3 * 60 * 60,
            'execution_date_fn': ignore_past,
        },
        'premium_crosssell_mpsignup': {
            'task_id': 'premium_crosssell_followup_1_sensor',
            'external_dag_id': 'athena_production_tables.athena_premium_crosssell_mpsignup',
            'external_task_id': 'completed',
            'poke_interval': 90,
            'mode': 'reschedule',
            'timeout': 3 * 60 * 60,
            'execution_date_fn': ignore_past,
        },
        'paying_subscriptions': {
            'task_id': 'paying_subscriptions_sensor',
            'external_dag_id': 'paying_subscriptions.athena_paying_subscriptions',
            'external_task_id': 'completed',
            'poke_interval': 90,
            'mode': 'reschedule',
            'timeout': 2 * 60 * 60,
            'execution_date_fn': ignore_past,
        }
    }

    templated_dags = [
        'marketplace_services',
        'zuora_subscriptions',
        'active_users',
        'subscription_events',
        'users',
        'portfolios'
    ]
    for dag in templated_dags:
        sensors_config.update({
            dag: {
                'task_id': '{dag}_sensor'.format(dag=dag),
                'external_dag_id': '{dag}.athena_{dag}'.format(dag=dag),
                'external_task_id': 'completed',
                'poke_interval': 90,
                'mode': 'reschedule',
                'timeout': 3 * 60 * 60,
                'execution_date_fn': ignore_past,
            }
        })

    sensors = {}

    for schema in ['dbr', 'dbl', 'mariadb', 'zuora', 'zuora_stateful', 'dbl_backfill', 'for_sapi']:
        for table in dependencies.get(schema, []):
            if table in ('page_events', 'email_events'):
                eval_schema = lambda \
                        s: table if schema == 'dbl' else table + '_hist' if schema == 'dbl_backfill' else None
                sensors.update({
                    "{}.{}".format(schema, table): {
                        'task_id': '{}_sensor'.format(eval_schema(schema)),
                        'external_dag_id': 'emr_{}'.format(eval_schema(schema)),
                        'external_task_id': 'check_temp_results',
                        'poke_interval': 90,
                        'mode': 'reschedule',
                        'timeout': 3 * 60 * 60,
                        'execution_date_fn': ignore_past,
                    }
                })
            elif schema in ['dbr', 'dbl', 'for_sapi']:
                if table == 'sessions_ab_test':
                    sensors.update({"{}.{}".format(schema, table): sensors_config['sessions_ab_test_maria_dim']})
                    sensors.update({"{}.{}".format(schema, table): sensors_config['sessions_ab_test_page_event']})
                else:
                    sensors.update({"{}.{}".format(schema, table): sensors_config[table]})
            elif schema == 'mariadb':
                # mariadb dependencies
                sensors.update({
                    "{}.{}".format(schema, table): {
                        'task_id': 'mariadb_' + table + '_sensor',
                        'external_dag_id': 'mariadb_dump.athena_' + table,
                        'external_task_id': 'completed',
                        'poke_interval': 90,
                        'mode': 'reschedule',
                        'timeout': 2 * 60 * 60,
                        'execution_date_fn': ignore_past,
                    }
                })
            elif schema == 'zuora':
                # zuora dependencies
                sensors.update({
                    "{}.{}".format(schema, table): {
                        'task_id': 'zuora_sensor',
                        'external_dag_id': 'zuora_json',
                        'external_task_id': 'export_completed',
                        'poke_interval': 90,
                        'mode': 'reschedule',
                        'timeout': 1 * 60 * 60,
                        'execution_date_fn': ignore_past,
                    }
                })
            elif schema == 'zuora_stateful':
                # zuora dependencies
                sensors.update({
                    "{}.{}".format(schema, table): {
                        'task_id': 'zuora_sensor',
                        'external_dag_id': 'zuora_dump',
                        'external_task_id': 'record_last_partition',
                        'poke_interval': 90,
                        'mode': 'reschedule',
                        'soft_fail': True,
                        'timeout': 1 * 60 * 60,
                        'execution_date_fn': ignore_past,
                    }
                })

    if params:
        for table, new_params in params.items():
            if new_params.pop('depends_on_hourly', None):
                new_params.update({
                    'execution_date_fn': hourly_fn
                })
            if new_params.pop('depends_on_hourly_one_earlier', None):
                new_params.update({
                    'execution_date_fn': hourly_one_earlier_fn
                })
            if new_params.pop('depends_on_daily', None):
                new_params.update({
                    'execution_date_fn': daily_fn
                })
            if new_params.pop('depends_on_mariadb', None):
                new_params.update({
                    'execution_date_fn': mariadb_fn
                })

            sensors[table].update(new_params)

    print(sensors)


# ###############################
# dependencies and sensors
# ###############################
subscription_events_params = {
    'dbr.subscription_events': {'depends_on_hourly': True},
}

subscription_events_dep = {
    'dbr': ['subscription_events']
}
get_athena_dependencies(subscription_events_dep, subscription_events_params)

active_users_params = {
    'dbr.active_users': {'depends_on_hourly_one_earlier': True},
}
active_users_dep = {
    'dbr': ['active_users']
}
get_athena_dependencies(active_users_dep, active_users_params)
