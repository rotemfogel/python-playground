# Returns element closest to target in arr[]
from pendulum import Pendulum


def hourly_fn(execution_date):
    """
    default function for sensors of daily DAGs that depend on hourly DAGs
    assuming the daily task runs at 05:25
    :param execution_date: the DAG execution date
    :return: Pendulum
    """

    execution_date_transformed = execution_date.add(days=1)

    return execution_date_transformed


def daily_fn(execution_date):
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
    hour = execution_date.hour
    closest_execution_date = execution_date.subtract(days=1).hour_(5).minute_(25).second_(0).microsecond_(0)
    if hour < 5:
        return closest_execution_date.subtract(days=1)
    return closest_execution_date


def _find_closest_hour(execution_date, hours):
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


def mariadb_fn(execution_date):
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


def other_fn(execution_date):
    arr = [0, 7, 20]
    return _find_closest_hour(execution_date, arr)


_last_post_diff = 15


def foo(execution_date: Pendulum,
        last_post_date: Pendulum):
    empty_slots = 0
    should_post = empty_slots == 0
    diff_passed = execution_date.subtract(minutes=_last_post_diff) >= last_post_date
    # if enough time passed since the last post,
    # or it's the first time it ran, save the current time
    return should_post and diff_passed


now = Pendulum(year=2020, month=4, day=22, minute=25)

for i in range(5):
    p = now.add(minutes=i * 5)
    r = foo(p, now)
    print("{} -> {}".format(p, r))

print("")

for i in range(23):
    p = now.add(hours=i)
    r = mariadb_fn(p)
    print("{} -> {}".format(p, r))

print("")

for i in range(23):
    p = now.add(hours=i)
    r = other_fn(p)
    print("{} -> {}".format(p, r))
