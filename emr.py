import copy
from pprint import pprint

import yaml
from pendulum import Pendulum

with open('page_events.yaml', 'r') as conf_file:
    events_process_conf = yaml.safe_load(conf_file)
emr_json = events_process_conf['emr_json']
sessions_step = events_process_conf['sessions_step']


def session_emr_config(global_emr_conf: dict) -> dict:
    full_emr_json = copy.deepcopy(global_emr_conf)
    session_step = copy.deepcopy(sessions_step[-1])
    session_step['Name'] = 'Sessions Pre Process - Spark application'
    hadoop_jar_steps = session_step['HadoopJarStep']
    steps = hadoop_jar_steps['Args']
    # replace page_events HDFS read to S3
    ix = steps.index('--page-events-input-path') + 1
    steps[ix] = 's3://{{ var.value.data_bucket }}/{{ var.json.schema.db_layer }}/page_events'
    # change date-hour-inputs
    ix = steps.index("--date-hour-inputs") + 1
    steps[ix] = Pendulum.now().subtract(hours=1).format('%Y-%m-%d-%H')
    # steps[ix] = sessions_date_hour_inputs(
    #    context['dag_run'].start_date if context['dag_run'] else macros.datetime.now(),
    #    context['execution_date'].subtract(hours=1))
    # write back configuration
    hadoop_jar_steps['Args'] = steps
    session_step['HadoopJarStep'] = hadoop_jar_steps
    emr_steps = full_emr_json['emr_json']['Steps']
    emr_steps.insert(2, session_step)
    print('added "Sessions Pre Process - Spark application" to EMR configuration')
    return full_emr_json['emr_json']


def process_session():
    session_emr = session_emr_config(events_process_conf)
    pprint(events_process_conf['emr_json'])
    pprint("-------------------------------------------------------------------------------")
    pprint(session_emr)


page_events = 'time spark-submit --deploy-mode cluster --conf spark.executor.extraJavaOptions=-XX:+UseG1GC --conf spark.sql.caseSensitive=true --class com.seekingalpha.dm_etl_spark.event.PageEvent target/dm_etl_spark.jar --date-hour-inputs %s,%s --date-hour-enrichment-inputs %s,%s --db-name dbraw --mone-posts-table mone_posts --mone-events-table mone_events --output-path-event hdfs:///page_events/'
session = 'time spark-submit --deploy-mode cluster --conf spark.executor.extraJavaOptions=-XX:+UseG1GC --conf spark.sql.caseSensitive=true --class com.seekingalpha.dm_etl_spark.event.SessionExtractor target/dm_etl_spark.jar --date-hour-inputs %s --page-events-input-path hdfs:///page_events/ --sessions-path s3://seekingalpha-data/dbl/sessions/ --read-format parquet --user-identifier machine_cookie'
enrichment = 'time spark-submit --deploy-mode cluster --conf spark.executor.extraJavaOptions=-XX:+UseG1GC --conf spark.sql.caseSensitive=true --class com.seekingalpha.dm_etl_spark.event.PageEventEnrichment target/dm_etl_spark.jar --date-hour-inputs %s,%s --target-input-path hdfs:///page_events/ --target-output-path s3://seekingalpha-data/dbl/page_events/ --sessions-path s3://seekingalpha-data/dbl/sessions/ --enrichment-sources session,paying_indication,experience_execute,amp_authentication --sessions-user-identifier-type machine_cookie --paying-subscriptions-table-name paying_subscriptions --paying-subscriptions-database-name dbr'
tasks = [{'name': 'page_events', 'task': page_events},
         {'name': 'session', 'task': session},
         {'name': 'enrichment', 'task': enrichment}]

if __name__ == "__main__":
    start = Pendulum(2021, 1, 17, 5)
    end = Pendulum(2021, 1, 18, 4)
    hours_ahead = 24
    while start < end:
        next_hour = start.add(hours=hours_ahead)
        start_fmt = start.format('%Y-%m-%d-%H')
        next_fmt = next_hour.format('%Y-%m-%d-%H')
        print(f'echo -e "{start_fmt}, {next_fmt}"')
        for task in tasks:
            task_name = task['name']
            task_cmd = task['task']
            if task_name in ['session', 'enrichment']:
                if task_name == 'enrichment':
                    print(copy.deepcopy(task_cmd) % (start_fmt, next_fmt))
                else:
                    for h in range(0, hours_ahead + 1):
                        print(copy.deepcopy(task_cmd) % start.add(hours=h).format('%Y-%m-%d-%H'))
            else:
                day_ago = start.subtract(days=1).format('%Y-%m-%d-%H')
                print(copy.deepcopy(task_cmd) % (start_fmt, next_fmt, day_ago, next_fmt))
                print(f"\nhadoop fs -ls /page_events/category=page_view/date_={start.format('%Y-%m-%d')}")
            print('')
        start = next_hour
        print('hadoop fs -rm -r -f /page_events')
        print('')
        print('')
        print('')
        print('')
