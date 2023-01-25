import os

import boto3
import pandas as pd
import pendulum
from dotenv import load_dotenv, find_dotenv
from pendulum import DateTime

if __name__ == "__main__":
    load_dotenv(find_dotenv())
    MIN_GB_TO_LOG = 50.0
    WORKGROUP = "primary"
    MAX_ITEMS = 100
    PAGE_SIZE = 50
    LOWER_TIME_LIMIT: DateTime = DateTime.utcnow().start_of("hour").subtract(hours=1)
    LAST_QUERY_EXECUTION = ""
    DATA_SCANNED_IN_BYTES = "DataScannedInBytes"
    records = []
    athena_client = boto3.client("athena")
    first = True
    last_query_token = os.getenv("LAST_TOKEN")
    force_break = False
    token = last_query_token
    while True:
        # break whenever paginator reached the end (no next token)
        if not first and not token:
            break
        first = False
        if token:
            query_executions = athena_client.list_query_executions(
                WorkGroup=WORKGROUP, MaxResults=PAGE_SIZE, NextToken=token
            )
        else:
            query_executions = athena_client.list_query_executions(
                WorkGroup=WORKGROUP, MaxResults=PAGE_SIZE
            )
        results = athena_client.batch_get_query_execution(
            QueryExecutionIds=query_executions["QueryExecutionIds"]
        )
        if results:
            # create a tuple of the query and the diff in minutes from LOWER_TIME_LIMIT
            query_times = list(
                map(
                    lambda q: (
                        q,
                        LOWER_TIME_LIMIT.diff(
                            pendulum.instance(q["Status"]["SubmissionDateTime"]),
                            abs=False,
                        ).minutes
                        if q["Status"].get("SubmissionDateTime")
                        else -1,
                    ),
                    results["QueryExecutions"],
                )
            )
            # if all queries are below LOWER_TIME_LIMIT, break
            if all(list(map(lambda x: x[-1] < 0, query_times))):
                break
            # filter the queries with diff > 0 and take the query object
            queries = list(
                map(
                    lambda q: q[0],
                    list(
                        filter(
                            lambda x: x[0]["Status"]["State"]
                            in ["SUCCEEDED", "FAILED", "CANCELLED"]
                            and float(
                                x[0]
                                .get("Statistics", {DATA_SCANNED_IN_BYTES: 0})
                                .get(DATA_SCANNED_IN_BYTES)
                                / pow(1024, 3)
                            )
                            > MIN_GB_TO_LOG
                            and x[-1] > 0,
                            query_times,
                        )
                    ),
                )
            )

            for query in queries:
                query_execution_id = query.get("QueryExecutionId")
                if query_execution_id:
                    statistics = query["Statistics"]
                    bytes_scanned = statistics[DATA_SCANNED_IN_BYTES]
                    records.append(
                        {
                            "query_execution_id": query_execution_id,
                            "query": query["Query"],
                            "query_type": query["StatementType"],
                            "database": query["QueryExecutionContext"]["Database"],
                            "submission_time": query["Status"][
                                "SubmissionDateTime"
                            ].timestamp(),
                            "bytes_scanned": bytes_scanned,
                            "execution_time_millis": statistics[
                                "EngineExecutionTimeInMillis"
                            ],
                            "planning_time_millis": statistics.get(
                                "QueryPlanningTimeInMillis", -1
                            ),
                            "queue_time_millis": statistics["QueryQueueTimeInMillis"],
                            "processing_time_millis": statistics[
                                "ServiceProcessingTimeInMillis"
                            ],
                            "total_execution_time_millis": statistics[
                                "TotalExecutionTimeInMillis"
                            ],
                            "status": query["Status"]["State"],
                            "workgroup": query["WorkGroup"],
                        }
                    )
        token = query_executions.get("NextToken")
        if token:
            last_query_token = token
            print("fetching next batch")
        if len(records) > 100:
            break

    if records:
        print(f"got {len(records)} queries")
        for i in range(0, len(records)):
            records[i].update({"id": i})
        df = pd.DataFrame(records)
        df.to_parquet(
            "/tmp/athena_query_stats.parquet",
            engine="pyarrow",
            allow_truncated_timestamps=True,
        )
