import time

import boto3


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
    'database': 'poc_dbraw',
    'bucket': 'aws-athena-query-results-714212176917-us-west-2',
    'path': 'airflow-output',
    'query': 'SELECT 1'
}


def execute():
    session = boto3.Session(profile_name='staging')
    client = session.client('athena', region_name='us-west-2')
    f = open('/home/rotem/query.sql', 'r')
    for line in f:
        if not line.startswith('--'):
            params.update({'query': line})
            print(params['query'])
            execution = athena_query(client, params)
            execution_id = execution['QueryExecutionId']
            state = 'RUNNING'

            while state in ['RUNNING', 'QUEUED']:
                response = client.get_query_execution(QueryExecutionId=execution_id)
                if 'QueryExecution' in response and \
                        'Status' in response['QueryExecution'] and \
                        'State' in response['QueryExecution']['Status']:
                    state = response['QueryExecution']['Status']['State']
                    print(state)
                    if state == 'FAILED':
                        print(response['QueryExecution']['Status']['StateChangeReason'])
                        break
                    elif state == 'SUCCEEDED':
                        break
                    time.sleep(1)

    f.close()
    # print(partitions)


if __name__ == '__main__':
    execute()
