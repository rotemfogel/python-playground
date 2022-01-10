import boto3
import pandas as pd
import pendulum
from pendulum import DateTime

if __name__ == "__main__":
    MIN_GB_TO_LOG = 50.0
    WORKGROUP = 'primary'
    MAX_ITEMS = 100
    PAGE_SIZE = 50
    LOWER_TIME_LIMIT: DateTime = DateTime.utcnow().start_of('hour').subtract(hours=1)
    LAST_QUERY_EXECUTION = ''
    DATA_SCANNED_IN_BYTES = 'DataScannedInBytes'
    records = []
    athena_client = boto3.client('athena')
    paginator = athena_client.get_paginator('list_query_executions')
    first = True
    last_token = None
    force_break = False
    token = last_token
    while True:
        # break whenever paginator reached the end (no next token)
        if not first and not token:
            break
        first = False
        pages = paginator.paginate(
            WorkGroup=WORKGROUP,
            PaginationConfig={
                'MaxItems': MAX_ITEMS,
                'PageSize': PAGE_SIZE,
                'StartingToken': token
            }
        )
        for page in pages:
            results = athena_client.batch_get_query_execution(QueryExecutionIds=page['QueryExecutionIds'])
            if results:
                queries = list(filter(lambda x: x['Status']['State'] in ['SUCCEEDED', 'FAILED', 'CANCELLED'] and
                                                float(x.get('Statistics', {DATA_SCANNED_IN_BYTES: 0}).get(
                                                    DATA_SCANNED_IN_BYTES) / pow(1024, 3)) > MIN_GB_TO_LOG,
                                      results['QueryExecutions']))
                for query in queries:
                    query_execution_id = query.get('QueryExecutionId')
                    if query_execution_id:
                        submission_time = query['Status'].get('SubmissionDateTime')
                        if submission_time:
                            # if query submission type falls on the previous hour, break
                            # assuming queries are ordered by submission date
                            if LOWER_TIME_LIMIT.diff(pendulum.instance(submission_time), abs=False).minutes < 0:
                                break
                            statistics = query['Statistics']
                            bytes_scanned = statistics[DATA_SCANNED_IN_BYTES]
                            records.append({
                                'query_execution_id': query_execution_id,
                                'query': query['Query'],
                                'query_type': query['StatementType'],
                                'database': query['QueryExecutionContext']['Database'],
                                'submission_time': submission_time.timestamp(),
                                'bytes_scanned': bytes_scanned,
                                'execution_time_millis': statistics['EngineExecutionTimeInMillis'],
                                'planning_time_millis': statistics['QueryPlanningTimeInMillis'],
                                'queue_time_millis': statistics['QueryQueueTimeInMillis'],
                                'processing_time_millis': statistics['ServiceProcessingTimeInMillis'],
                                'total_execution_time_millis': statistics['TotalExecutionTimeInMillis'],
                                'status': query['Status']['State'],
                                'workgroup': query['WorkGroup']
                            })
            token = page.get('NextToken')
            if token:
                last_token = token
                print('fetching next batch')
            if len(records) > 100:
                break
    if records:
        print(f'got {len(records)} queries')
        for i in range(0, len(records)):
            records[i].update({'id': i})
        df = pd.DataFrame(records)
        df.to_parquet('/tmp/athena_query_stats.parquet',
                      engine='pyarrow', allow_truncated_timestamps=True)
