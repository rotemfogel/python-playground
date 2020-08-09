import time

import boto3


def athena_query(client, params):
    response = client.start_query_execution(
        QueryString=params["query"],
        QueryExecutionContext={
            'Database': params['database']
        },
        ResultConfiguration={
            'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
        }
    )
    return response


params = {
    'region': 'us-west-2',
    'database': 'dbl',
    'bucket': 'aws-athena-query-results-744522205193-us-west-2',
    'path': 'airflow',
    'query': '???'
}

session = boto3.Session()
client = session.client('athena')
f = open('athena.sql')
lines = f.readlines()
f.close()

for line in lines:
    params.update({'query': line})
    execution = athena_query(client, params)

    execution_id = execution['QueryExecutionId']
    state = 'RUNNING'
    c = True
    while c and state in ['RUNNING']:
        response = client.get_query_execution(QueryExecutionId=execution_id)
        state = response['QueryExecution']['Status']['State']
        if state == 'FAILED':
            change_reason = str(response['QueryExecution']['Status']['StateChangeReason'])
            if 'Partition already exists' in change_reason:
                c = False
                break
        statistics = response['QueryExecution']['Statistics']
        if state == 'FAILED':
            c = False
        elif state == 'SUCCEEDED':
            c = False
        print('state: {}, {} millis'.format(state, statistics['TotalExecutionTimeInMillis']))
        time.sleep(1)
