import copy
import time

import boto3
from pendulum import DateTime


def athena_query(_client, _params):
    _response = _client.start_query_execution(
        QueryString=_params["query"],
        QueryExecutionContext={
            'Database': _params['database']
        },
        ResultConfiguration={
            'OutputLocation': 's3://' + _params['bucket'] + '/' + _params['path']
        }
    )
    return _response


params = {
    'region': 'us-west-2',
    'database': 'test_dbl',
    'bucket': 'aws-athena-query-results-744522205193-us-west-2',
    'path': 'airflow-output',
    'query': 'SELECT 1'
}


def get_client():
    session = boto3.Session(profile_name='default')
    return session.client('athena', region_name='us-west-2')


def execute_query(client, query: str):
    params.update({'query': query})
    # print(params['query'])
    execution = athena_query(client, params)
    execution_id = execution['QueryExecutionId']
    state = 'RUNNING'

    while state in ['RUNNING', 'QUEUED']:
        response = client.get_query_execution(QueryExecutionId=execution_id)
        if 'QueryExecution' in response and \
                'Status' in response['QueryExecution'] and \
                'State' in response['QueryExecution']['Status']:
            state = response['QueryExecution']['Status']['State']
            # print(state)
            if state in ('RUNNING', 'QUEUED'):
                pass
            elif state == 'FAILED':
                print(params['query'])
                message = response['QueryExecution']['Status']['StateChangeReason']
                print(message)
                raise Exception(message)
            elif state == 'SUCCEEDED':
                break
            else:
                raise Exception(state)
            time.sleep(1)


def line_by_line():
    client = get_client()
    f = open('/home/rotem/query.sql', 'r')
    for line in f:
        if not line.startswith('--'):
            execute_query(client, line)

    f.close()


def whole():
    client = get_client()
    f = open('/home/rotem/query.sql', 'r')
    query = f.read()
    f.close()
    start = DateTime(2021, 1, 1, 0)
    end = DateTime(2021, 1, 6, 10)
    while start < end:
        print(start)
        hour_query = copy.deepcopy(query) % (start.format('%Y-%m-%d'), start.format("%H"))
        execute_query(client, hour_query)
        start = start.add(hours=1)


if __name__ == '__main__':
    whole()
