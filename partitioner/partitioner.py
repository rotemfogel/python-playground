import sys

import boto3

_params = {
    "region": "us-west-2",
    "database": "dbl",
    "bucket": "aws-athena-query-results-744522205193-us-west-2",
    "path": "airflow",
    "query": "???",
}


def athena_query(client, params):
    response = client.start_query_execution(
        QueryString=_params["query"],
        QueryExecutionContext={"Database": _params["database"]},
        ResultConfiguration={
            "OutputLocation": "s3://" + params["bucket"] + "/" + params["path"]
        },
    )
    return response


def main(file):
    f = open(file)
    lines = f.readlines()
    f.close()

    session = boto3.Session()
    client = session.client("athena")

    for line in lines:
        _params.update({"query": line})
        execution = athena_query(client, _params)

        execution_id = execution["QueryExecutionId"]
        state = "RUNNING"
        while state in ["RUNNING"]:
            response = client.get_query_execution(QueryExecutionId=execution_id)
            state = response["QueryExecution"]["Status"]["State"]
            statistics = response["QueryExecution"]["Statistics"]
            print(
                "id: {}, state: {}, {} millis".format(
                    execution_id, state, statistics["TotalExecutionTimeInMillis"]
                )
            )
            if state == "FAILED":
                change_reason = str(
                    response["QueryExecution"]["Status"]["StateChangeReason"]
                )
                if "Partition already exists" in change_reason:
                    print("{}: partition already exists", _params["query"])
                    break
                else:
                    print(change_reason)
                    break
            elif state == "SUCCEEDED":
                break
            # time.sleep(1)


if __name__ == "__main__":
    main(sys.argv[1])
