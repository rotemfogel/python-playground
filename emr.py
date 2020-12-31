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
    steps[ix] = Pendulum.now().subtract(hours=1).format("%Y-%m-%d-%H")
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


if __name__ == "__main__":
    session_emr = session_emr_config(events_process_conf)
    pprint(events_process_conf['emr_json'])
    pprint("-------------------------------------------------------------------------------")
    pprint(session_emr)
